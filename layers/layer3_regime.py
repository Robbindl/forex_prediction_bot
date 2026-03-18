"""
layers/layer3_regime.py — Market regime + order flow layer.

Changes vs original:
  - Order flow imbalance is ONLY fetched for crypto assets.
    For forex/indices/commodities, it returns 0.0 (not applicable).
  - All exceptions properly logged (no silent return 0.0).
  - Sources tracking written to signal.metadata.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from core.signal import Signal
from core.signal_journal import PASS, KILLED
from core.asset_profiles import get_profile
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
    except Exception as e:
        logger.error(f"[RegimeLayer] Regime detection failed: {e}")
        return "unknown"


def _get_orderflow_imbalance(asset: str, profile) -> float:
    """
    Pull bid/ask imbalance from Phase 3 order flow.
    Returns 0.0 (neutral) if order flow is not applicable or unavailable.
    NEVER raises — logs errors instead.
    """
    if not profile.use_order_flow:
        logger.debug(f"[RegimeLayer] Order flow not applicable for {asset} ({profile.category})")
        return 0.0
    try:
        from order_flow import get_imbalance
        symbol = (asset.replace("-USD", "USDT")
                       .replace("/", "")
                       .replace("-", ""))
        imbalance = get_imbalance(symbol)
        if imbalance is None:
            logger.warning(f"[RegimeLayer] Order flow returned None for {asset}")
            return 0.0
        return float(imbalance)
    except Exception as e:
        logger.error(f"[RegimeLayer] Order flow fetch failed for {asset}: {e}")
        return 0.0


class RegimeLayer:
    name = "regime"

    _ALLOWED: Dict[str, set] = {
        "BUY":  {"trending_up",   "ranging", "unknown"},
        "SELL": {"trending_down", "ranging", "unknown"},
    }

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        profile     = get_profile(signal.asset)

        df     = context.get("price_data")
        regime = _detect_regime(df) if df is not None else context.get("regime", "unknown")
        signal.metadata["regime"] = regime

        # ── Order flow imbalance (crypto only) ────────────────────────────
        imbalance = _get_orderflow_imbalance(signal.asset, profile)
        signal.metadata["orderflow_imbalance"] = round(imbalance, 3)
        signal.metadata["orderflow_applicable"] = profile.use_order_flow

        # ── Regime gate ───────────────────────────────────────────────────
        allowed = self._ALLOWED.get(signal.direction, {"unknown"})
        if regime not in allowed:
            reason  = f"regime '{regime}' conflicts with {signal.direction}"
            penalty = 0.08
            signal.reduce(penalty)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"regime": regime,
                      "imbalance": round(imbalance, 3),
                      "penalty": penalty,
                      "orderflow_applicable": profile.use_order_flow},
            )
            logger.log_pipeline(signal.asset, LAYER, "REGIME_PENALTY", reason)

        if regime == "volatile":
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

        # Order flow adjustment — only when imbalance came from real data
        if profile.use_order_flow and imbalance != 0.0:
            direction_sign = 1 if signal.direction == "BUY" else -1
            if imbalance * direction_sign > 0.30:
                signal.boost(0.02)
            elif imbalance * direction_sign < -0.30:
                signal.reduce(0.02)

        reason = f"regime={regime}  imbalance={imbalance:+.3f}  orderflow={'yes' if profile.use_order_flow else 'n/a'}"
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "regime":               regime,
                "imbalance":            round(imbalance, 3),
                "orderflow_applicable": profile.use_order_flow,
                "phase3":               "order_flow" if profile.use_order_flow else "not_applicable",
            },
        )
        logger.log_pipeline(
            signal.asset, LAYER, "PASS",
            f"regime={regime} imbalance={imbalance:+.3f}"
        )
        return signal
