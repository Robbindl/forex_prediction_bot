"""Layer 3 — Market regime filter. Rewritten from market_regime_analyzer.py."""
from __future__ import annotations
from typing import Any, Dict, Optional
import pandas as pd
import numpy as np
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 3


def _detect_regime(df: pd.DataFrame) -> str:
    """Simple regime detection: trending / ranging / volatile."""
    if df is None or len(df) < 30:
        return "unknown"
    try:
        close   = df["close"].astype(float)
        sma20   = close.rolling(20).mean()
        sma50   = close.rolling(50).mean() if len(df) >= 50 else sma20
        returns = close.pct_change().dropna()
        vol     = returns.std() * np.sqrt(252)

        if vol > 0.4:
            return "volatile"
        if sma20.iloc[-1] > sma50.iloc[-1] and close.iloc[-1] > sma20.iloc[-1]:
            return "trending_up"
        if sma20.iloc[-1] < sma50.iloc[-1] and close.iloc[-1] < sma20.iloc[-1]:
            return "trending_down"
        return "ranging"
    except Exception:
        return "unknown"


class RegimeLayer:
    name = "regime"

    # direction → allowed regimes
    _ALLOWED: Dict[str, set] = {
        "BUY":  {"trending_up",   "ranging", "unknown"},
        "SELL": {"trending_down", "ranging", "unknown"},
    }

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        df     = context.get("price_data")
        regime = _detect_regime(df) if df is not None else context.get("regime", "unknown")

        signal.metadata["regime"] = regime

        allowed = self._ALLOWED.get(signal.direction, {"unknown"})
        if regime not in allowed:
            signal.kill(f"Regime '{regime}' conflicts with {signal.direction}", LAYER)
            return None

        if regime in ("trending_up", "trending_down"):
            signal.boost(0.03)

        if regime == "volatile":
            signal.kill("Volatile regime — skipping", LAYER)
            return None

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"regime={regime}")
        return signal