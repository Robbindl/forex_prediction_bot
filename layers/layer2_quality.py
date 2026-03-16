"""
layers/layer2_quality.py — Signal quality gate.

Checks R:R ratio and spread/liquidity proxy.
Writes full decision to signal.journal.
"""
from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger = get_logger()
LAYER        = 2
MIN_RR       = 1.5
MAX_SPREAD_PCT = 0.005


class QualityLayer:
    name = "quality"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        data        = {}

        # ── R:R check ────────────────────────────────────────────────────
        rr = 0.0
        if signal.entry_price and signal.stop_loss and signal.take_profit:
            risk   = abs(signal.entry_price - signal.stop_loss)
            reward = abs(signal.take_profit - signal.entry_price)
            rr     = reward / risk if risk > 0 else 0.0
            signal.risk_reward = round(rr, 2)
            data["rr"] = round(rr, 2)

            if rr < MIN_RR:
                reason = f"R:R {rr:.2f} below minimum {MIN_RR}"
                signal.kill(reason, LAYER)
                signal.journal.record(
                    layer=LAYER, name=self.name, decision=KILLED,
                    reason=reason,
                    conf_before=conf_before, conf_after=signal.confidence,
                    data=data,
                )
                return None

        # ── Spread / liquidity proxy ──────────────────────────────────────
        spread = context.get("spread")
        price  = signal.entry_price
        spread_pct = 0.0
        if spread and price and price > 0:
            spread_pct = spread / price
            data["spread_pct"] = round(spread_pct, 5)
            if spread_pct > MAX_SPREAD_PCT:
                penalty = min(0.15, spread_pct * 10)
                signal.reduce(penalty)
                signal.metadata["spread_penalty"] = round(penalty, 4)
                data["spread_penalty"] = round(penalty, 4)
                logger.log_pipeline(
                    signal.asset, LAYER, "SPREAD_PENALTY",
                    f"spread={spread_pct:.4f} penalty={penalty:.4f}"
                )
                if signal.confidence < 0.5:
                    reason = f"spread {spread_pct:.4f} killed confidence below 0.5"
                    signal.kill(reason, LAYER)
                    signal.journal.record(
                        layer=LAYER, name=self.name, decision=KILLED,
                        reason=reason,
                        conf_before=conf_before, conf_after=signal.confidence,
                        data=data,
                    )
                    return None

        reason = f"RR={rr:.2f}  spread={spread_pct:.4f}"
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data=data,
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"rr={signal.risk_reward}")
        return signal