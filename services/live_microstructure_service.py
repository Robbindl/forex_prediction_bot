from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Tuple


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


def _normalize_provider(provider: str) -> str:
    token = str(provider or "").strip().lower()
    if token.startswith("ig"):
        return "ig"
    if token.startswith("deriv"):
        return "deriv"
    if token.startswith("binance"):
        return "binance"
    return token or "unknown"


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

        event = {
            "timestamp": _safe_ts(timestamp),
            "price": float(price_value),
            "bid": bid_value,
            "ask": ask_value,
            "bid_size": _safe_float(bid_size, 0.0) if bid_size not in (None, "") else None,
            "ask_size": _safe_float(ask_size, 0.0) if ask_size not in (None, "") else None,
            "levels": normalized_levels,
            "flags": str(flags or "").strip(),
        }
        with self._lock:
            bucket = self._quotes.setdefault((provider_key, asset_key), deque(maxlen=self._maxlen))
            bucket.append(event)

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
        current_price = _safe_float(price, latest.get("price", 0.0))
        if current_price <= 0.0:
            return {}

        current_bid = latest.get("bid")
        current_ask = latest.get("ask")
        if current_bid in (None, 0.0) and current_ask in (None, 0.0):
            raw_spread = _safe_float(spread, 0.0)
        else:
            raw_spread = max(0.0, _safe_float(current_ask, current_price) - _safe_float(current_bid, current_price))
        spread_bps = round((raw_spread / current_price) * 10000.0, 3) if current_price > 0 else 0.0

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

        baseline_spread_bps = spread_bps
        if spread_history:
            try:
                baseline_spread_bps = statistics.median(spread_history[:-1] or spread_history)
            except Exception:
                baseline_spread_bps = spread_history[-1]
        spread_stress = (spread_bps / max(baseline_spread_bps, 0.01)) if baseline_spread_bps > 0 else 1.0

        bid_depth = 0.0
        ask_depth = 0.0
        depth_levels = 0
        if latest.get("levels"):
            for level in latest["levels"]:
                bid_size_level = _safe_float(level.get("bid_size"), 0.0)
                ask_size_level = _safe_float(level.get("ask_size"), 0.0)
                if bid_size_level > 0 or ask_size_level > 0:
                    depth_levels += 1
                bid_depth += max(0.0, bid_size_level)
                ask_depth += max(0.0, ask_size_level)
        else:
            bid_depth = max(0.0, _safe_float(latest.get("bid_size"), 0.0))
            ask_depth = max(0.0, _safe_float(latest.get("ask_size"), 0.0))
            depth_levels = 1 if (bid_depth > 0 or ask_depth > 0) else 0

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
                if baseline_spread_bps > 0.0:
                    spread_pressure = (baseline_spread_bps - spread_bps) / baseline_spread_bps
                velocity_sign = 1.0 if velocity_bps > 0 else (-1.0 if velocity_bps < 0 else (1.0 if latest_delta_bps > 0 else (-1.0 if latest_delta_bps < 0 else 0.0)))
                synthetic_book_imbalance = _clip11(
                    tick_imbalance * 0.45
                    + _clip11(quote_skew) * 0.30
                    + _clip11(spread_pressure) * velocity_sign * 0.25
                )
                book_imbalance = synthetic_book_imbalance

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
        if "EDIT" in str(latest.get("flags", "")).upper():
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
            "provider": provider_key,
            "spread_bps": round(spread_bps, 3),
            "tick_imbalance": round(_clip11(tick_imbalance), 4),
            "book_imbalance": round(_clip11(book_imbalance), 4),
            "synthetic_book_imbalance": round(_clip11(synthetic_book_imbalance), 4),
            "velocity_bps": round(velocity_bps, 4),
            "latest_delta_bps": round(latest_delta_bps, 4),
            "spread_stress": round(max(0.0, spread_stress), 4),
            "stop_hunt_risk": round(stop_hunt_risk, 4),
            "exhaustion_risk": round(exhaustion_risk, 4),
            "pressure_direction": pressure_direction,
            "depth_available": bool(total_depth > 0),
            "synthetic_depth_available": bool(synthetic_depth_available),
            "depth_levels": int(depth_levels),
            "quote_updates": int(len(events)),
            "score": round(score, 4),
            "microstructure_source": "live_store_depth" if total_depth > 0 else ("live_store_synthetic_depth" if synthetic_depth_available else "live_store"),
        }

    def clear(self) -> None:
        with self._lock:
            self._quotes.clear()


_service = LiveMicrostructureService()


def get_service() -> LiveMicrostructureService:
    return _service
