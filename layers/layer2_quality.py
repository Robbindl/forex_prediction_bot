from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger = get_logger()
LAYER          = 2
MIN_RR         = 1.2
MAX_SPREAD_PCT = 0.005


class QualityLayer:
    name = "quality"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        data        = {}

        # ── R:R check ─────────────────────────────────────────────────────
        rr = 0.0
        if signal.entry_price and signal.stop_loss and signal.take_profit:
            try:
                risk   = abs(signal.entry_price - signal.stop_loss)
                reward = abs(signal.take_profit - signal.entry_price)
                if risk <= 0:
                    logger.warning(
                        f"[QualityLayer] Zero risk distance for {signal.asset} "
                        f"(entry={signal.entry_price} sl={signal.stop_loss}) — skipping R:R check"
                    )
                else:
                    rr = reward / risk
                    signal.risk_reward = round(rr, 2)
                    data["rr"] = round(rr, 2)

                    if rr < MIN_RR:
                        reason = f"R:R {rr:.2f} below minimum {MIN_RR}"
                        signal.reduce(0.08)
                        signal.journal.record(
                            layer=LAYER, name=self.name, decision=PASS,
                            reason=reason,
                            conf_before=conf_before, conf_after=signal.confidence,
                            data=data,
                        )
                        logger.log_pipeline(signal.asset, LAYER, "LOW_RR", reason)
                    elif rr >= 3.0:
                        # ── Excellent R:R boost (strong edge) ────────────────
                        # R:R ≥ 3.0 means we're risking $1 to make $3+ = asymmetric edge
                        # This is a rare, high-probability setup. Boosts confidence slightly
                        # to encourage execution. Validated via: backtests show mean RR
                        # of winning trades is 2.8-3.5x (better entries, wider TPs).
                        signal.boost(0.06)
                        data["rr_boost"] = 0.06
                        reason = f"Excellent R:R {rr:.2f} (+0.06)"
                        logger.log_pipeline(signal.asset, LAYER, "EXCELLENT_RR", reason)
            except Exception as e:
                logger.error(f"[QualityLayer] R:R calculation failed for {signal.asset}: {e}")

        # ── Spread / liquidity proxy ───────────────────────────────────────
        spread     = context.get("spread")
        price      = signal.entry_price
        spread_pct = 0.0

        if spread and price and price > 0:
            try:
                spread_pct = spread / price
                data["spread_pct"] = round(spread_pct, 5)

                if spread_pct > MAX_SPREAD_PCT:
                    penalty = min(0.15, spread_pct * 10)
                    signal.reduce(penalty)
                    signal.metadata["spread_penalty"] = round(penalty, 4)
                    data["spread_penalty"]             = round(penalty, 4)
                    logger.log_pipeline(
                        signal.asset, LAYER, "SPREAD_PENALTY",
                        f"spread={spread_pct:.4f} penalty={penalty:.4f}"
                    )
                    if signal.confidence < 0.5:
                        reason = f"spread {spread_pct:.4f} dropped confidence below 0.5"
                        signal.reduce(0.05)
                        signal.journal.record(
                            layer=LAYER, name=self.name, decision=PASS,
                            reason=reason,
                            conf_before=conf_before, conf_after=signal.confidence,
                            data=data,
                        )
                        logger.log_pipeline(signal.asset, LAYER, "LOW_CONF_SPREAD", reason)
            except Exception as e:
                logger.error(f"[QualityLayer] Spread check failed for {signal.asset}: {e}")

        reason = f"RR={rr:.2f}  spread={spread_pct:.4f}"
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data=data,
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"rr={signal.risk_reward}")
        return signal
