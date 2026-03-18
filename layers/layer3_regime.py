from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger = get_logger()
LAYER = 3


def _detect_regime(df: pd.DataFrame) -> str:
    if df is None or len(df) < 30:
        return "unknown"
    try:
        close   = df["close"].astype(float)
        sma20   = close.rolling(20).mean()
        sma50   = close.rolling(50).mean() if len(df) >= 50 else sma20
        returns = close.pct_change().dropna()

        # Annualise volatility correctly for the active timeframe
        # Daily=252, 1h=252*24=6048, 15m=252*96=24192, 4h=252*6=1512
        try:
            from config.config import TRADING_TIMEFRAME
            _bars_per_year = {"15m": 24192, "1h": 6048, "4h": 1512, "1d": 252}
            ann_factor = _bars_per_year.get(TRADING_TIMEFRAME, 252)
        except Exception:
            ann_factor = 252
        vol = returns.std() * np.sqrt(ann_factor)

        if vol > 0.4:
            return "volatile"
        if sma20.iloc[-1] > sma50.iloc[-1] and close.iloc[-1] > sma20.iloc[-1]:
            return "trending_up"
        if sma20.iloc[-1] < sma50.iloc[-1] and close.iloc[-1] < sma20.iloc[-1]:
            return "trending_down"
        return "ranging"
    except Exception:
        return "unknown"


def _get_orderflow_imbalance(asset: str) -> float:
    """
    Pull bid/ask imbalance score from Phase 3 order flow.
    Returns -1.0 (sell pressure) to +1.0 (buy pressure), 0.0 if unavailable.
    """
    try:
        from order_flow import get_imbalance
        # Normalise asset name to exchange format (BTC-USD → BTCUSDT)
        symbol = asset.replace("-USD", "USDT").replace("/", "").replace("-", "")
        return get_imbalance(symbol)
    except Exception:
        return 0.0


class RegimeLayer:
    name = "regime"

    _ALLOWED: Dict[str, set] = {
        "BUY":  {"trending_up",   "ranging", "unknown"},
        "SELL": {"trending_down", "ranging", "unknown"},
    }

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence

        df     = context.get("price_data")
        regime = _detect_regime(df) if df is not None else context.get("regime", "unknown")
        signal.metadata["regime"] = regime

        # ── Phase 3: Order flow imbalance ─────────────────────────────────
        imbalance = _get_orderflow_imbalance(signal.asset)
        signal.metadata["orderflow_imbalance"] = round(imbalance, 3)

        # ── Regime gate ───────────────────────────────────────────────────
        allowed = self._ALLOWED.get(signal.direction, {"unknown"})
        if regime not in allowed:
            reason = f"regime '{regime}' conflicts with {signal.direction}"
            penalty = 0.08
            signal.reduce(penalty)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"regime": regime, "imbalance": round(imbalance, 3), "penalty": penalty},
            )
            logger.log_pipeline(signal.asset, LAYER, "REGIME_PENALTY", reason)
        
        if regime == "volatile":
            # Volatile regime is less reliable; lower confidence instead of killing.
            signal.reduce(0.1)
            reason = "volatile regime — confidence penalty"
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"regime": regime},
            )
            logger.log_pipeline(signal.asset, LAYER, "VOLATILE_PENALTY", reason)

        # ── Confidence adjustments ────────────────────────────────────────
        if regime in ("trending_up", "trending_down"):
            signal.boost(0.03)

        # Order flow confirms direction → small boost
        direction_sign = 1 if signal.direction == "BUY" else -1
        if imbalance * direction_sign > 0.30:
            signal.boost(0.02)
        elif imbalance * direction_sign < -0.30:
            signal.reduce(0.02)

        reason = f"regime={regime}  imbalance={imbalance:+.3f}"
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "regime":    regime,
                "imbalance": round(imbalance, 3),
                "phase3":    "order_flow" if imbalance != 0.0 else "unavailable",
            },
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS", f"regime={regime} imbalance={imbalance:+.3f}")
        return signal