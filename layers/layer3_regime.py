"""
layers/layer3_regime.py — Market regime filter.

Detects trending / ranging / volatile regime from price data.
NOW WIRED TO PHASE 3: reads order flow imbalance score from
order_flow module to enrich regime context.

Writes full decision to signal.journal including Phase 3 data.
"""
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
            signal.kill(reason, LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"regime": regime, "imbalance": round(imbalance, 3)},
            )
            return None

        if regime == "volatile":
            signal.kill("volatile regime — skipping", LAYER)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=KILLED,
                reason="volatile regime",
                conf_before=conf_before, conf_after=signal.confidence,
                data={"regime": regime},
            )
            return None

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