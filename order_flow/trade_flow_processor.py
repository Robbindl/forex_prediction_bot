from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, Optional


def _clip11(value: float) -> float:
    return max(-1.0, min(1.0, float(value or 0.0)))


def _normalize_side(value: object) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "BULL", "LONG"}:
        return "BUY"
    if raw in {"SELL", "BEAR", "SHORT"}:
        return "SELL"
    return ""


class TradeFlowProcessor:
    """
    Rolling crypto trade-flow tracker built from public trade updates.

    This is not a full institutional tape engine, but it gives the bot a much
    better directional read than flat book imbalance alone by tracking:
    - recent aggressive buy vs sell notional
    - rolling trade delta
    - cumulative volume delta (CVD) over the current process lifetime
    """

    _MAX_TRADES = 160
    _MAX_CVD_POINTS = 64

    def __init__(self, asset: str) -> None:
        self.asset = asset
        self._lock = threading.RLock()
        self._trades: Deque[Dict[str, float]] = deque(maxlen=self._MAX_TRADES)
        self._cvd_points: Deque[Dict[str, float]] = deque(maxlen=self._MAX_CVD_POINTS)
        self._cumulative_delta = 0.0
        self._latest_snapshot: Dict[str, float] = {}

    def ingest_trade(
        self,
        *,
        price: object,
        qty: object,
        side: object,
        timestamp: object = None,
    ) -> Dict[str, float]:
        trade_price = float(price or 0.0)
        trade_qty = float(qty or 0.0)
        trade_side = _normalize_side(side)
        if trade_price <= 0.0 or trade_qty <= 0.0 or not trade_side:
            return self.latest_snapshot()

        trade_ts = self._normalize_ts(timestamp)
        notional = trade_price * trade_qty
        signed_notional = notional if trade_side == "BUY" else -notional

        with self._lock:
            self._cumulative_delta += signed_notional
            self._trades.append(
                {
                    "ts": trade_ts,
                    "price": trade_price,
                    "qty": trade_qty,
                    "notional": notional,
                    "signed_notional": signed_notional,
                    "side": 1.0 if trade_side == "BUY" else -1.0,
                }
            )
            self._cvd_points.append({"ts": trade_ts, "cvd": self._cumulative_delta})
            self._latest_snapshot = self._build_snapshot_locked()
            return dict(self._latest_snapshot)

    def latest_snapshot(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._latest_snapshot)

    def current_score(self) -> float:
        with self._lock:
            return float(self._latest_snapshot.get("trade_flow_score", 0.0) or 0.0)

    @staticmethod
    def _normalize_ts(value: object) -> float:
        try:
            numeric = float(value or 0.0)
            if numeric > 10_000_000_000:
                numeric /= 1000.0
            if numeric > 1_000_000:
                return numeric
        except Exception:
            pass
        return time.time()

    def _build_snapshot_locked(self) -> Dict[str, float]:
        trades = list(self._trades)
        if not trades:
            return {}

        buy_notional = sum(item["notional"] for item in trades if item["side"] > 0)
        sell_notional = sum(item["notional"] for item in trades if item["side"] < 0)
        trade_delta = buy_notional - sell_notional
        total_notional = buy_notional + sell_notional
        trade_delta_ratio = (trade_delta / total_notional) if total_notional > 0 else 0.0

        buy_count = sum(1 for item in trades if item["side"] > 0)
        sell_count = sum(1 for item in trades if item["side"] < 0)
        trade_count = buy_count + sell_count

        if len(self._cvd_points) >= 2:
            cvd_change = self._cvd_points[-1]["cvd"] - self._cvd_points[0]["cvd"]
        else:
            cvd_change = trade_delta
        cvd_slope = (cvd_change / total_notional) if total_notional > 0 else 0.0

        trade_flow_score = _clip11(trade_delta_ratio * 0.68 + cvd_slope * 0.32)
        pressure_direction = "NEUTRAL"
        if trade_flow_score >= 0.12:
            pressure_direction = "BUY"
        elif trade_flow_score <= -0.12:
            pressure_direction = "SELL"

        latest_ts = trades[-1]["ts"]
        return {
            "trade_buy_notional": round(buy_notional, 4),
            "trade_sell_notional": round(sell_notional, 4),
            "trade_delta_notional": round(trade_delta, 4),
            "trade_delta_ratio": round(_clip11(trade_delta_ratio), 4),
            "trade_cvd": round(self._cumulative_delta, 4),
            "trade_cvd_slope": round(_clip11(cvd_slope), 4),
            "trade_flow_score": round(trade_flow_score, 4),
            "trade_pressure_direction": pressure_direction,
            "trade_buy_count": int(buy_count),
            "trade_sell_count": int(sell_count),
            "trade_count": int(trade_count),
            "trade_ts": latest_ts,
            "trade_live_age_seconds": round(max(0.0, time.time() - latest_ts), 3),
        }
