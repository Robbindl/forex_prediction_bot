"""
Layer 7 — Final calibration: spread/liquidity proxy, position sizing, SL/TP refinement.
Replaces synthetic orderflow engine entirely.
"""
from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 7

_MIN_FINAL_CONFIDENCE = 0.60
_MAX_SPREAD_PCT       = 0.003   # 0.3% hard kill threshold at final layer


class CalibrationLayer:
    name = "calibration"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        price  = signal.entry_price
        spread = context.get("spread")

        # ── Spread / liquidity proxy ──────────────────────────────────────
        if spread and price and price > 0:
            liquidity = spread / price          # wider spread = lower liquidity
            if liquidity > _MAX_SPREAD_PCT:
                signal.kill(
                    f"Final spread check: {liquidity:.5f} > {_MAX_SPREAD_PCT}", LAYER
                )
                return None
            # Reduce confidence proportionally to spread
            liq_penalty = liquidity / _MAX_SPREAD_PCT * 0.05
            signal.reduce(liq_penalty)
            signal.metadata["liquidity_proxy"] = round(liquidity, 6)

        # ── Final confidence floor ────────────────────────────────────────
        if signal.confidence < _MIN_FINAL_CONFIDENCE:
            signal.kill(
                f"Final confidence {signal.confidence:.3f} < {_MIN_FINAL_CONFIDENCE}", LAYER
            )
            return None

        # ── Position sizing via risk manager ──────────────────────────────
        engine = context.get("engine")
        if engine and hasattr(engine, "_risk_manager") and engine._risk_manager:
            try:
                rm   = engine._risk_manager
                size = rm.calculate_position_size(
                    entry_price=signal.entry_price,
                    stop_loss=signal.stop_loss,
                    category=signal.category,
                )
                signal.position_size = size
                signal.risk_parameters["position_size"] = size
            except Exception as e:
                logger.debug(f"[Layer7] Position sizing error: {e}")

        # ── Add take profit levels (3-tier) ──────────────────────────────
        if signal.entry_price and signal.take_profit and not signal.take_profit_levels:
            entry = signal.entry_price
            tp1   = signal.take_profit
            dist  = abs(tp1 - entry)
            if signal.direction == "BUY":
                signal.take_profit_levels = [
                    round(entry + dist * 0.5, 6),
                    round(entry + dist,       6),
                    round(entry + dist * 1.5, 6),
                ]
            else:
                signal.take_profit_levels = [
                    round(entry - dist * 0.5, 6),
                    round(entry - dist,       6),
                    round(entry - dist * 1.5, 6),
                ]

        signal.layer_reached = LAYER
        logger.log_pipeline(
            signal.asset, LAYER, "PASS",
            f"conf={signal.confidence:.3f} size={signal.position_size:.4f}"
        )
        return signal