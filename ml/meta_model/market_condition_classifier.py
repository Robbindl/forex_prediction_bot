from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from utils.logger import get_logger

logger = get_logger()


class MarketConditionClassifier:
    """
    Stateless regime classifier. Call classify() on every signal.
    Uses price data from the pipeline context + macro signals from Phase 1.
    """

    # ── Thresholds ────────────────────────────────────────────────────────────
    ADX_TREND_THRESHOLD  = 25.0    # ADX above this = trending
    ADX_RANGE_THRESHOLD  = 20.0    # ADX below this = ranging
    VOL_HIGH_THRESHOLD   = 0.80    # annualised vol > 80% = high volatility
    VOL_CRISIS_THRESHOLD = 1.50    # annualised vol > 150% = crisis

    def classify(
        self,
        df:              Optional[pd.DataFrame] = None,
        funding_bias:    str   = "NEUTRAL",    # from Phase 1 FundingRateMonitor
        oi_signal:       str   = "NEUTRAL",    # from Phase 1 OIMonitor
        macro_impact:    str   = "LOW",        # from Phase 1 MacroDataCollector
        narrative_str:   float = 0.0,          # from Phase 4 TopicClusterEngine
    ) -> str:
        """
        Classify market regime. Returns one of:
        trending_bull | trending_bear | ranging | high_volatility | crisis
        """
        # ── Crisis check first (highest priority) ────────────────────────
        if macro_impact == "HIGH" and narrative_str > 0.3:
            return "crisis"

        if df is None or len(df) < 30:
            return "ranging"   # safe default with no data

        try:
            close  = df["close"].astype(float)
            ret    = close.pct_change().dropna()
            vol    = float(ret.rolling(20).std().iloc[-1]) * np.sqrt(252)

            if vol >= self.VOL_CRISIS_THRESHOLD:
                return "crisis"
            if vol >= self.VOL_HIGH_THRESHOLD:
                return "high_volatility"

            adx    = self._calc_adx(df)
            ema20  = float(close.ewm(span=20, adjust=False).mean().iloc[-1])
            ema50  = float(close.ewm(span=50, adjust=False).mean().iloc[-1])
            price  = float(close.iloc[-1])

            if adx >= self.ADX_TREND_THRESHOLD:
                if price > ema20 > ema50:
                    return "trending_bull"
                if price < ema20 < ema50:
                    return "trending_bear"

            # Extreme funding = over-leveraged market = higher volatility risk
            if funding_bias in ("EXTREME_LONG", "EXTREME_SHORT"):
                return "high_volatility"

            return "ranging"

        except Exception as e:
            logger.debug(f"[RegimeClassifier] classify error: {e}")
            return "ranging"

    def classify_from_context(self, context: dict) -> str:
        """
        Convenience method — pulls all inputs from the pipeline context dict.
        Called by EnsemblePredictor.
        """
        return self.classify(
            df           = context.get("price_data"),
            funding_bias = context.get("funding_bias",   "NEUTRAL"),
            oi_signal    = context.get("oi_signal",      "NEUTRAL"),
            macro_impact = context.get("macro_impact",   "LOW"),
            narrative_str= context.get("narrative_strength", 0.0),
        )

    def get_regime_description(self, regime: str) -> str:
        return {
            "trending_bull":   "Strong uptrend — ADX > 25, price above EMAs",
            "trending_bear":   "Strong downtrend — ADX > 25, price below EMAs",
            "ranging":         "Sideways market — low ADX, oscillating price",
            "high_volatility": "Elevated volatility — unstable conditions",
            "crisis":          "Crisis conditions — extreme vol or macro shock",
        }.get(regime, "Unknown regime")

    @staticmethod
    def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
        try:
            high  = df["high"].astype(float)
            low   = df["low"].astype(float)
            close = df["close"].astype(float)

            plus_dm  = high.diff().clip(lower=0)
            minus_dm = (-low.diff()).clip(lower=0)
            tr = pd.concat([
                high - low,
                (high - close.shift()).abs(),
                (low  - close.shift()).abs(),
            ], axis=1).max(axis=1)

            atr      = tr.rolling(period).mean().replace(0, np.nan)
            plus_di  = 100 * plus_dm.rolling(period).mean()  / atr
            minus_di = 100 * minus_dm.rolling(period).mean() / atr
            dx       = (100 * (plus_di - minus_di).abs()
                        / (plus_di + minus_di).replace(0, np.nan))
            adx      = dx.rolling(period).mean()
            return float(adx.iloc[-1]) if not adx.empty else 0.0
        except Exception:
            return 0.0