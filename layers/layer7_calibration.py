from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger
from config.config import MIN_FINAL_CONFIDENCE

logger = get_logger()
LAYER = 7

_MAX_SPREAD_PCT = 0.003


class CalibrationLayer:
    name = "calibration"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        price       = signal.entry_price
        spread      = context.get("spread")
        data        = {}

        # ── Spread / liquidity proxy ──────────────────────────────────────
        if spread and price and price > 0:
            liquidity = spread / price
            data["liquidity_proxy"] = round(liquidity, 6)
            if liquidity > _MAX_SPREAD_PCT:
                reason = f"final spread {liquidity:.5f} > {_MAX_SPREAD_PCT}"
                signal.kill(reason, LAYER)
                signal.journal.record(
                    layer=LAYER, name=self.name, decision=KILLED,
                    reason=reason,
                    conf_before=conf_before, conf_after=signal.confidence,
                    data=data,
                )
                return None
            liq_penalty = liquidity / _MAX_SPREAD_PCT * 0.05
            signal.reduce(liq_penalty)
            signal.metadata["liquidity_proxy"] = round(liquidity, 6)
            data["liq_penalty"] = round(liq_penalty, 5)

        # ── Final confidence floor ────────────────────────────────────────
        if signal.confidence <= MIN_FINAL_CONFIDENCE:
            reason = f"final conf {signal.confidence:.3f} below floor {MIN_FINAL_CONFIDENCE}"
            signal.kill(reason, LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data=data,
            )
            return None

        # ── Position sizing ───────────────────────────────────────────────
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
                data["position_size"] = round(size, 6)
            except Exception as e:
                logger.debug(f"[Layer7] Position sizing error: {e}")

        # ── 3-tier take profit levels ─────────────────────────────────────
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
        data["final_conf"] = round(signal.confidence, 4)

        reason = (
            f"final conf={signal.confidence:.3f}  "
            f"size={signal.position_size:.4f}  "
            f"tp_levels={len(signal.take_profit_levels)}"
        )
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data=data,
        )
        logger.log_pipeline(
            signal.asset, LAYER, "PASS",
            f"conf={signal.confidence:.3f} size={signal.position_size:.4f}"
        )
        return signal