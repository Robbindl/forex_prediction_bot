"""Layer 2 — Signal quality: R:R ratio, ATR filter, spread/liquidity proxy."""
from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 2
MIN_RR    = 1.5
MAX_SPREAD_PCT = 0.005   # 0.5% max spread relative to price


class QualityLayer:
    name = "quality"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        # ── R:R check ────────────────────────────────────────────────────
        if signal.entry_price and signal.stop_loss and signal.take_profit:
            risk   = abs(signal.entry_price - signal.stop_loss)
            reward = abs(signal.take_profit - signal.entry_price)
            rr     = reward / risk if risk > 0 else 0.0
            signal.risk_reward = round(rr, 2)

            if rr < MIN_RR:
                signal.kill(f"R:R {rr:.2f} < {MIN_RR}", LAYER)
                return None

        # ── Spread / liquidity proxy ──────────────────────────────────────
        spread = context.get("spread")
        price  = signal.entry_price
        if spread and price and price > 0:
            spread_pct = spread / price
            if spread_pct > MAX_SPREAD_PCT:
                # Wide spread — reduce confidence proportionally
                penalty = min(0.15, spread_pct * 10)
                signal.reduce(penalty)
                signal.metadata["spread_penalty"] = round(penalty, 4)
                logger.log_pipeline(
                    signal.asset, LAYER, "SPREAD_PENALTY",
                    f"spread={spread_pct:.4f} penalty={penalty:.4f}"
                )
                if signal.confidence < 0.5:
                    signal.kill(f"Spread {spread_pct:.4f} killed confidence", LAYER)
                    return None

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"rr={signal.risk_reward}")
        return signal