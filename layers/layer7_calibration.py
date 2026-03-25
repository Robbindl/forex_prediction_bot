from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger
from config.config import MIN_FINAL_CONFIDENCE, SPREAD_THRESHOLDS

logger = get_logger()
LAYER = 7


class CalibrationLayer:
    name = "calibration"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        price       = signal.entry_price
        spread      = context.get("spread")
        category    = context.get("category", "forex")
        data        = {}

        # Get category-specific spread threshold; default to forex (0.002) if category not in config
        max_spread_pct = SPREAD_THRESHOLDS.get(category, 0.002)

        # ── Final spread gate ─────────────────────────────────────────────
        if spread and price and price > 0:
            try:
                liquidity = spread / price
                data["liquidity_proxy"] = round(liquidity, 6)
                if liquidity > max_spread_pct:
                    reason = f"final spread {liquidity:.5f} > {max_spread_pct} ({category})"
                    signal.kill(reason, LAYER)
                    signal.journal.record(
                        layer=LAYER, name=self.name, decision=KILLED,
                        reason=reason,
                        conf_before=conf_before, conf_after=signal.confidence,
                        data=data,
                    )
                    return None
                liq_penalty = liquidity / max_spread_pct * 0.05
                signal.reduce(liq_penalty)
                signal.metadata["liquidity_proxy"] = round(liquidity, 6)
                data["liq_penalty"] = round(liq_penalty, 5)
            except Exception as e:
                logger.error(f"[CalibrationLayer] Spread gate failed for {signal.asset}: {e}")

        # ── Final confidence floor ─────────────────────────────────────────
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

        # ── Position sizing ────────────────────────────────────────────────
        engine = context.get("engine")
        if engine and hasattr(engine, "_risk_manager") and engine._risk_manager:
            try:
                size = engine._risk_manager.calculate_position_size(
                    entry_price = signal.entry_price,
                    stop_loss   = signal.stop_loss,
                    category    = signal.category,
                    asset       = signal.asset,
                )
                signal.position_size                    = size
                signal.risk_parameters["position_size"] = size
                data["position_size"]                   = round(size, 6)
            except Exception as e:
                logger.error(f"[CalibrationLayer] Position sizing failed for {signal.asset}: {e}")

        # ── 3-tier take-profit levels ──────────────────────────────────────
        if signal.entry_price and signal.take_profit and not signal.take_profit_levels:
            try:
                entry = signal.entry_price
                tp1   = signal.take_profit
                dist  = abs(tp1 - entry)
                if dist > 0:
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
                else:
                    logger.warning(
                        f"[CalibrationLayer] Zero TP distance for {signal.asset} "
                        f"(entry={entry} tp={tp1}) — skipping TP levels"
                    )
            except Exception as e:
                logger.error(f"[CalibrationLayer] TP level calculation failed for {signal.asset}: {e}")

        signal.layer_reached  = LAYER
        data["final_conf"]    = round(signal.confidence, 4)

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