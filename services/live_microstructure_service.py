from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple
import math
import statistics
import threading
import time

from services.dom_evidence import attach_dom_evidence


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_ts(value: Any = None) -> float:
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return float(dt.timestamp())
    if value in (None, ""):
        return time.time()
    return _safe_float(value, time.time())


def _normalize_provider(provider: str) -> str:
    return str(provider or "").strip().lower() or "unknown"


def _flag_tokens(value: Any) -> set[str]:
    return {part.strip().lower() for part in str(value or "").replace("|", ",").split(",") if part.strip()}


def _clip(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _depth_tier(score: float) -> str:
    if score >= 0.72:
        return "strong"
    if score >= 0.48:
        return "usable"
    if score > 0.0:
        return "thin"
    return "none"


def estimate_true_depth_metrics(
    levels: Optional[Iterable[Dict[str, Any]]] = None,
    *,
    bid_size: Any = None,
    ask_size: Any = None,
) -> Dict[str, Any]:
    bid_depth = 0.0
    ask_depth = 0.0
    bid_levels = 0
    ask_levels = 0

    for level in list(levels or []):
        if not isinstance(level, dict):
            continue
        bsz = max(0.0, _safe_float(level.get("bid_size"), 0.0))
        asz = max(0.0, _safe_float(level.get("ask_size"), 0.0))
        if bsz > 0.0:
            bid_depth += bsz
            bid_levels += 1
        if asz > 0.0:
            ask_depth += asz
            ask_levels += 1

    if bid_depth <= 0.0 and ask_depth <= 0.0:
        bid_depth = max(0.0, _safe_float(bid_size, 0.0))
        ask_depth = max(0.0, _safe_float(ask_size, 0.0))
        bid_levels = 1 if bid_depth > 0.0 else 0
        ask_levels = 1 if ask_depth > 0.0 else 0

    total = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total if total > 0.0 else 0.0
    depth_levels = max(bid_levels, ask_levels)
    quality = 0.0
    if total > 0.0:
        quality = min(1.0, 0.18 + min(depth_levels, 10) * 0.065 + min(math.log10(total + 1.0), 3.0) * 0.08)
    return {
        "depth_available": bool(total > 0.0),
        "synthetic_depth_available": False,
        "bid_depth": round(bid_depth, 6),
        "ask_depth": round(ask_depth, 6),
        "bid_vol": round(bid_depth, 6),
        "ask_vol": round(ask_depth, 6),
        "total_depth": round(total, 6),
        "depth_levels": int(depth_levels),
        "bid_level_count": int(bid_levels),
        "ask_level_count": int(ask_levels),
        "book_imbalance": round(_clip(imbalance), 4),
        "depth_quality": round(quality, 4),
        "depth_quality_tier": _depth_tier(quality),
    }


class LiveMicrostructureService:
    def __init__(self, maxlen: int = 96) -> None:
        self._lock = threading.RLock()
        self._maxlen = max(8, int(maxlen))
        self._quotes: Dict[Tuple[str, str], Deque[Dict[str, Any]]] = {}

    def clear(self) -> None:
        with self._lock:
            self._quotes.clear()

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
        price_value = _safe_float(price, 0.0)
        if price_value <= 0.0 and bid_value and ask_value:
            price_value = (bid_value + ask_value) / 2.0
        if price_value <= 0.0:
            price_value = ask_value or bid_value or 0.0
        if price_value <= 0.0:
            return

        normalized_levels: List[Dict[str, Any]] = []
        for level in levels or []:
            if not isinstance(level, dict):
                continue
            normalized_levels.append(
                {
                    "bid": _safe_float(level.get("bid"), 0.0) if level.get("bid") not in (None, "") else None,
                    "ask": _safe_float(level.get("ask"), 0.0) if level.get("ask") not in (None, "") else None,
                    "bid_size": _safe_float(level.get("bid_size"), 0.0)
                    if level.get("bid_size") not in (None, "")
                    else None,
                    "ask_size": _safe_float(level.get("ask_size"), 0.0)
                    if level.get("ask_size") not in (None, "")
                    else None,
                }
            )

        token_set = _flag_tokens(flags)
        event = str(event_type or "").strip().lower()
        if not event:
            if "depth_delta" in token_set or "ladder_delta" in token_set:
                event = "depth_delta"
            elif normalized_levels or bid_size not in (None, "") or ask_size not in (None, ""):
                event = "depth_snapshot"
            elif trade_size not in (None, ""):
                event = "trade_print"
            else:
                event = "quote"

        with self._lock:
            self._quotes.setdefault((provider_key, asset_key), deque(maxlen=self._maxlen)).append(
                {
                    "timestamp": _safe_ts(timestamp),
                    "price": float(price_value),
                    "bid": bid_value,
                    "ask": ask_value,
                    "bid_size": _safe_float(bid_size, 0.0) if bid_size not in (None, "") else None,
                    "ask_size": _safe_float(ask_size, 0.0) if ask_size not in (None, "") else None,
                    "levels": normalized_levels,
                    "flags": str(flags or ""),
                    "trade_size": _safe_float(trade_size, 0.0) if trade_size not in (None, "") else None,
                    "trade_side": str(trade_side or "").strip().lower(),
                    "event_type": event,
                }
            )

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
    def _is_depth_event(event: Dict[str, Any]) -> bool:
        flags = _flag_tokens(event.get("flags"))
        return bool(
            event.get("levels")
            or event.get("bid_size") not in (None, "")
            or event.get("ask_size") not in (None, "")
            or str(event.get("event_type") or "") in {"depth_snapshot", "depth_delta"}
            or flags.intersection({"depth_snapshot", "stream_snapshot", "depth_delta", "ladder_delta", "book_delta"})
        )

    @staticmethod
    def _depth_update_mode(event: Dict[str, Any]) -> str:
        flags = _flag_tokens(event.get("flags"))
        event_type = str(event.get("event_type") or "").strip().lower()
        if "ladder_delta" in flags or event_type == "depth_delta":
            return "event_stream"
        if "stream_snapshot" in flags:
            return "stream_snapshot"
        if "depth_snapshot" in flags or event_type == "depth_snapshot":
            return "snapshot_poll"
        return "synthetic_proxy"

    @staticmethod
    def _series_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        prices = [_safe_float(evt.get("price"), 0.0) for evt in events if _safe_float(evt.get("price"), 0.0) > 0.0]
        deltas = [curr - prev for prev, curr in zip(prices, prices[1:]) if abs(curr - prev) > 1e-12]
        up = sum(1 for delta in deltas if delta > 0.0)
        down = sum(1 for delta in deltas if delta < 0.0)
        tick_imbalance = (up - down) / (up + down) if up + down else 0.0
        velocity_bps = 0.0
        if len(prices) >= 2 and prices[-1] > 0.0:
            velocity_bps = (prices[-1] - prices[0]) / prices[-1] * 10000.0
        buy_volume = 0.0
        sell_volume = 0.0
        for evt in events:
            if str(evt.get("event_type")) != "trade_print":
                continue
            size = max(0.0, _safe_float(evt.get("trade_size"), 0.0))
            side = str(evt.get("trade_side") or "").lower()
            if side.startswith("b"):
                buy_volume += size
            elif side.startswith("s"):
                sell_volume += size
        trade_total = buy_volume + sell_volume
        trade_flow = (buy_volume - sell_volume) / trade_total if trade_total > 0.0 else 0.0
        return {
            "prices": prices,
            "tick_imbalance": _clip(tick_imbalance),
            "velocity_bps": round(velocity_bps, 4),
            "trade_flow_score": round(_clip(trade_flow), 4),
            "trade_buy_volume": round(buy_volume, 6),
            "trade_sell_volume": round(sell_volume, 6),
        }

    @staticmethod
    def _event_counts(events: List[Dict[str, Any]], latest_depth_ts: float) -> Dict[str, int]:
        active_window = [evt for evt in events if _safe_float(evt.get("timestamp"), 0.0) >= latest_depth_ts - 20.0]
        return {
            "dom_snapshot_count": sum(
                1
                for evt in active_window
                if str(evt.get("event_type")) == "depth_snapshot"
                or "depth_snapshot" in _flag_tokens(evt.get("flags"))
                or "stream_snapshot" in _flag_tokens(evt.get("flags"))
            ),
            "dom_delta_count": sum(
                1
                for evt in active_window
                if str(evt.get("event_type")) == "depth_delta" or "ladder_delta" in _flag_tokens(evt.get("flags"))
            ),
            "dom_trade_count": sum(1 for evt in active_window if str(evt.get("event_type")) == "trade_print"),
        }

    @staticmethod
    def _ladder_proxy_metrics(events: List[Dict[str, Any]]) -> Dict[str, Any]:
        depth_events = [evt for evt in events if LiveMicrostructureService._is_depth_event(evt)]
        if len(depth_events) < 2:
            return {
                "dom_add_intent_bias": 0.0,
                "dom_cancel_pressure_bias": 0.0,
                "dom_queue_erosion_bias": 0.0,
                "dom_trade_absorption_proxy": 0.0,
                "dom_refill_after_sweep_bias": 0.0,
                "dom_iceberg_proxy": 0.0,
                "dom_trade_backed_iceberg_proxy": 0.0,
                "dom_trade_backed_iceberg_hits": 0,
                "dom_refill_after_sweep_hits": 0,
                "dom_queue_persistence": 0.0,
                "dom_liquidity_shift_proxy": 0.0,
                "dom_sweep_pressure_proxy": 0.0,
                "dom_refill_resilience_proxy": 0.0,
            }
        metrics = [estimate_true_depth_metrics(evt.get("levels"), bid_size=evt.get("bid_size"), ask_size=evt.get("ask_size")) for evt in depth_events]
        first = metrics[0]
        last = metrics[-1]
        bid_change = _safe_float(last.get("bid_depth")) - _safe_float(first.get("bid_depth"))
        ask_change = _safe_float(last.get("ask_depth")) - _safe_float(first.get("ask_depth"))
        total_last = max(_safe_float(last.get("total_depth")), 1e-9)
        add_intent = _clip((bid_change - ask_change) / total_last, 0.0, 1.0)
        cancel_pressure = _clip(abs(ask_change - bid_change) / total_last, 0.0, 1.0)
        imbalances = [_safe_float(item.get("book_imbalance")) for item in metrics]
        queue_persistence = 1.0 - min(1.0, statistics.pstdev(imbalances) if len(imbalances) > 1 else 0.0)
        trade_events = [evt for evt in events if str(evt.get("event_type")) == "trade_print"]
        trade_absorption = min(1.0, len(trade_events) / 4.0) * max(0.15, abs(imbalances[-1]))
        prev = metrics[-2] if len(metrics) >= 2 else first
        bid_refill = _safe_float(last.get("bid_depth")) > _safe_float(prev.get("bid_depth"))
        ask_refill = _safe_float(last.get("ask_depth")) > _safe_float(prev.get("ask_depth"))
        total_refill = total_last >= _safe_float(prev.get("total_depth"))
        refill_hits = 1 if len(depth_events) >= 3 and (bid_refill or ask_refill or total_refill) else 0
        iceberg_hits = 1 if trade_absorption > 0.05 and queue_persistence > 0.45 else 0
        return {
            "dom_add_intent_bias": round(add_intent, 4),
            "dom_cancel_pressure_bias": round(cancel_pressure, 4),
            "dom_queue_erosion_bias": round(max(0.0, 1.0 - queue_persistence), 4),
            "dom_trade_absorption_proxy": round(trade_absorption, 4),
            "dom_refill_after_sweep_bias": round(
                max(
                    0.0,
                    refill_hits
                    * min(
                        1.0,
                        0.18
                        + abs(_safe_float(last.get("bid_depth")) - _safe_float(prev.get("bid_depth"))) / max(total_last, 1e-9)
                        + abs(_safe_float(last.get("ask_depth")) - _safe_float(prev.get("ask_depth"))) / max(total_last, 1e-9),
                    ),
                ),
                4,
            ),
            "dom_iceberg_proxy": round(trade_absorption * queue_persistence, 4),
            "dom_trade_backed_iceberg_proxy": round(trade_absorption * queue_persistence if trade_events else 0.0, 4),
            "dom_trade_backed_iceberg_hits": int(iceberg_hits),
            "dom_refill_after_sweep_hits": int(refill_hits),
            "dom_queue_persistence": round(_clip(queue_persistence, 0.0, 1.0), 4),
            "dom_liquidity_shift_proxy": round(min(1.0, abs(bid_change) + abs(ask_change) / max(total_last, 1e-9)), 4),
            "dom_sweep_pressure_proxy": round(min(1.0, cancel_pressure + trade_absorption), 4),
            "dom_refill_resilience_proxy": round(min(1.0, queue_persistence * (1.0 if refill_hits else 0.55)), 4),
        }

    @staticmethod
    def _fragmentation(provider: str, asset: str, all_events: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        latest_by_provider = {
            venue: events[-1]
            for venue, events in all_events.items()
            if events and _safe_float(events[-1].get("price"), 0.0) > 0.0
        }
        prices = [_safe_float(evt.get("price"), 0.0) for evt in latest_by_provider.values()]
        if len(prices) < 2:
            return {
                "cross_venue_count": len(prices),
                "dom_fragmentation_provider_count": len(prices),
                "dom_fragmented_market": False,
                "cross_venue_price_dispersion_bps": 0.0,
                "dom_cross_venue_mid_dislocation_bps": 0.0,
                "dom_fragmentation_score": 0.0,
            }
        mid = statistics.median(prices)
        dispersion = (max(prices) - min(prices)) / mid * 10000.0 if mid > 0.0 else 0.0
        return {
            "cross_venue_count": len(prices),
            "dom_fragmentation_provider_count": len(prices),
            "dom_fragmented_market": bool(dispersion >= 6.0),
            "cross_venue_price_dispersion_bps": round(dispersion, 4),
            "dom_cross_venue_mid_dislocation_bps": round(dispersion, 4),
            "dom_fragmentation_score": round(min(1.0, dispersion / 20.0), 4),
        }

    @staticmethod
    def _stream_health(provider: str, asset: str) -> Dict[str, Any]:
        try:
            from services.dom_stream_health_service import get_service as get_dom_stream_health_service

            return dict(get_dom_stream_health_service().snapshot(provider, asset) or {})
        except Exception:
            return {
                "dom_stream_health_known": False,
                "dom_stream_connected": False,
                "dom_stream_degraded": False,
                "dom_stream_health_score": 1.0,
                "dom_stream_trust_decay": 0.0,
                "dom_stream_reconnect_count": 0,
                "dom_stream_sequence_gap_count": 0,
                "dom_stream_reason": "",
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
            all_events = {
                venue: list(bucket)
                for (venue, stored_asset), bucket in self._quotes.items()
                if stored_asset == asset_key and bucket
            }

        if not events and price in (None, "", 0):
            return {}
        latest = events[-1] if events else {"timestamp": time.time(), "price": _safe_float(price, 0.0)}
        depth_events = [evt for evt in events if self._is_depth_event(evt)]
        latest_depth = depth_events[-1] if depth_events else latest
        latest_depth_ts = _safe_float(latest_depth.get("timestamp"), _safe_float(latest.get("timestamp"), time.time()))
        recent_events = [evt for evt in events if _safe_float(evt.get("timestamp"), 0.0) >= latest_depth_ts - 20.0]

        current_price = _safe_float(price, latest.get("price"))
        if current_price <= 0.0:
            return {}
        bid_value = latest.get("bid") if latest.get("bid") not in (None, "") else latest_depth.get("bid")
        ask_value = latest.get("ask") if latest.get("ask") not in (None, "") else latest_depth.get("ask")
        raw_spread = _safe_float(spread, 0.0)
        if raw_spread <= 0.0 and bid_value not in (None, "") and ask_value not in (None, ""):
            raw_spread = max(0.0, _safe_float(ask_value) - _safe_float(bid_value))
        spread_bps = raw_spread / current_price * 10000.0 if current_price > 0.0 else 0.0

        series = self._series_metrics(recent_events or events)
        depth = estimate_true_depth_metrics(
            latest_depth.get("levels"),
            bid_size=latest_depth.get("bid_size"),
            ask_size=latest_depth.get("ask_size"),
        )
        counts = self._event_counts(events, latest_depth_ts)
        update_mode = self._depth_update_mode(latest_depth) if depth.get("depth_available") else "none"
        ladder = self._ladder_proxy_metrics(recent_events or events)
        health = self._stream_health(provider_key, asset_key)
        fragmentation = self._fragmentation(provider_key, asset_key, all_events)

        book = _safe_float(depth.get("book_imbalance"), 0.0)
        flow = _safe_float(series.get("trade_flow_score"), 0.0)
        tick = _safe_float(series.get("tick_imbalance"), 0.0)
        velocity = _clip(_safe_float(series.get("velocity_bps"), 0.0) / 8.0)
        score = _clip(book * 0.48 + flow * 0.26 + tick * 0.16 + velocity * 0.10)

        exchange_provider = provider_key in {"binance", "bybit", "okx"}
        broker_l2_provider = provider_key in {"ctrader", "dukascopy", "ig", "deriv"}
        depth_available = bool(depth.get("depth_available"))
        if not depth_available:
            feed_class = "quote_only"
        elif exchange_provider:
            feed_class = "exchange_deep"
        elif broker_l2_provider:
            depth_levels = int(depth.get("depth_levels", 0) or 0)
            feed_class = "broker_l2" if depth_levels >= 5 else "thin_broker_l2"
        else:
            feed_class = "unknown"

        payload: Dict[str, Any] = {
            "provider": provider_key,
            "asset": asset_key,
            "price": round(current_price, 8),
            "bid": bid_value,
            "ask": ask_value,
            "spread": round(raw_spread, 8),
            "spread_bps": round(spread_bps, 4),
            "microstructure_source": f"{provider_key}_live_depth" if depth_available else provider_key,
            "depth_provider": provider_key,
            "depth_provider_class": "exchange_depth"
            if exchange_provider
            else "broker_l2"
            if broker_l2_provider
            else "unknown",
            "depth_transport_class": "sidecar" if broker_l2_provider else "",
            "depth_feed_class": feed_class,
            "depth_normalization_scope": f"{asset_key}:{provider_key}:{feed_class}",
            "depth_max_expected_levels": 1000 if exchange_provider else 10 if broker_l2_provider else 0,
            "depth_provider_trust_score": 0.88
            if exchange_provider
            else 0.78
            if provider_key in {"dukascopy", "ig"}
            else 0.58
            if provider_key == "ctrader"
            else 0.62,
            "depth_quote_alignment_score": 1.0,
            "depth_quote_agreement_state": "aligned",
            "depth_update_mode": update_mode,
            "depth_live_age_seconds": round(max(0.0, time.time() - latest_depth_ts), 3),
            "score": round(score, 4),
            "microstructure_alignment": round(score, 4),
            "orderflow_imbalance": round(score, 4),
            "tick_imbalance": round(_safe_float(series.get("tick_imbalance")), 4),
            "velocity_bps": round(_safe_float(series.get("velocity_bps")), 4),
            "trade_flow_score": round(flow, 4),
            "trade_delta_ratio": round(flow, 4),
            "trade_cvd_slope": round(flow, 4),
            "flags": str(latest_depth.get("flags") or latest.get("flags") or ""),
        }
        payload.update(depth)
        payload.update(counts)
        payload.update(ladder)
        payload.update(health)
        payload.update(fragmentation)
        payload.update(dict(meta or {}))
        return attach_dom_evidence(payload)


_service = LiveMicrostructureService()


def get_service() -> LiveMicrostructureService:
    return _service
