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

        # ── Real-time entry quality boosts ────────────────────────────────
        # These three factors measure concurrent conditions that increase
        # trade probability beyond the layer-stacking logic. They do NOT
        # double-count with historical backtest boosting (PipelineReporter).
        # IMPORTANT: These thresholds (20%, 0.6x, 15%) are EMPIRICAL GUESSES.
        # They MUST be validated via backtesting before live deployment.
        # See VALIDATION_FRAMEWORK.md for backtest procedure.
        df = context.get("price_data")
        if df is not None and len(df) >= 20:
            try:
                close = df["close"].astype(float)
                high  = df["high"].astype(float)
                low   = df["low"].astype(float)
                
                # Martingale-favorable: entry within 20% of recent swing low
                recent_low  = low.iloc[-20:].min()
                recent_high = high.iloc[-20:].max()
                entry_range = recent_high - recent_low
                if entry_range > 0:
                    if signal.direction == "BUY":
                        dist_from_low = signal.entry_price - recent_low
                        if 0 <= dist_from_low <= entry_range * 0.20:
                            signal.boost(0.03)
                            data["martingale_boost"] = 0.03
                            reason_append = f"Martingale-favorable entry (within 20% of swing low)"
                            logger.log_pipeline(signal.asset, LAYER, "MARTINGALE_ENTRY", reason_append)
                    else:  # SELL
                        dist_from_high = recent_high - signal.entry_price
                        if 0 <= dist_from_high <= entry_range * 0.20:
                            signal.boost(0.03)
                            data["martingale_boost"] = 0.03
                            reason_append = f"Martingale-favorable entry (within 20% of swing high)"
                            logger.log_pipeline(signal.asset, LAYER, "MARTINGALE_ENTRY", reason_append)
                
                # Volatility crush: current bar volatility < recent average = mean-reversion setup
                current_range = high.iloc[-1] - low.iloc[-1]
                recent_avg_range = (high.iloc[-20:-1] - low.iloc[-20:-1]).mean()
                if recent_avg_range > 0:
                    volatility_ratio = current_range / recent_avg_range
                    if volatility_ratio < 0.6:  # bar is 40% less volatile than average
                        signal.boost(0.02)
                        data["volatility_crush_boost"] = 0.02
                        logger.log_pipeline(
                            signal.asset, LAYER, "VOLATILITY_CRUSH",
                            f"Current vol {volatility_ratio:.2f}x recent avg (< 0.6x)"
                        )
                
                # Support/resistance alignment: entry within recent swing extremes
                # (already closer to one side = stronger institutional level)
                swing_range = entry_range
                if swing_range > 0:
                    if signal.direction == "BUY":
                        proximity_to_support = (signal.entry_price - recent_low) / swing_range
                        if proximity_to_support < 0.15:  # very close to support
                            signal.boost(0.025)
                            data["support_alignment_boost"] = 0.025
                            logger.log_pipeline(
                                signal.asset, LAYER, "SUPPORT_ALIGNMENT",
                                f"Entry {proximity_to_support:.1%} above swing low"
                            )
                    else:  # SELL
                        proximity_to_resistance = (recent_high - signal.entry_price) / swing_range
                        if proximity_to_resistance < 0.15:  # very close to resistance
                            signal.boost(0.025)
                            data["resistance_alignment_boost"] = 0.025
                            logger.log_pipeline(
                                signal.asset, LAYER, "RESISTANCE_ALIGNMENT",
                                f"Entry {proximity_to_resistance:.1%} below swing high"
                            )
            except Exception as e:
                logger.debug(f"[CalibrationLayer] Real-time entry quality check failed for {signal.asset}: {e}")

        # ── Final spread gate ─────────────────────────────────────────────
        if spread and price and price > 0:
            try:
                liquidity = spread / price
                data["liquidity_proxy"] = round(liquidity, 6)
                logger.debug(f"[CalibrationLayer] {signal.asset} ({category}): spread={spread}, price={price}, liquidity={liquidity:.6f}, max_allowed={max_spread_pct}")
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