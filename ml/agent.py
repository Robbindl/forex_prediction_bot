"""
ml/agent.py — Policy trading agent for live decisions and reward-driven execution.
"""
from __future__ import annotations
import json
import threading
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
from core.confidence import clamp_confidence, squash_confidence
from utils.logger import get_logger
from ml.registry import registry
from ml.features import build_features
from core.signal import Signal

logger = get_logger()


def _parse_metadata(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _float_val(metadata: Dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        return float(metadata.get(key, default) or default)
    except Exception:
        return default


def _bool_val(metadata: Dict[str, Any], key: str) -> float:
    val = metadata.get(key)
    if isinstance(val, str):
        return 1.0 if val.lower() in ("true", "1", "yes", "real") else 0.0
    return 1.0 if val else 0.0


def _regime_to_numeric(value: Any) -> float:
    if isinstance(value, str):
        value = value.lower()
        if value in ("bull", "up", "long"):
            return 1.0
        if value in ("bear", "down", "short"):
            return -1.0
    return 0.0


def _dominant_to_numeric(value: Any) -> float:
    if isinstance(value, str):
        value = value.lower()
        if value in ("bull", "buy", "long"):
            return 1.0
        if value in ("bear", "sell", "short"):
            return -1.0
    return 0.0


def build_agent_features(df: pd.DataFrame, context: Dict[str, Any]) -> np.ndarray | None:
    features = build_features(df)
    if features is None:
        return None

    metadata = _parse_metadata(context.get("signal_metadata") or context.get("metadata") or {})
    extra = np.array([
        _float_val(metadata, "ml_confidence"),
        _bool_val(metadata, "ml_prediction_real"),
        _regime_to_numeric(metadata.get("regime")),
        _float_val(metadata, "sentiment_score"),
        _float_val(metadata, "reddit_score"),
        _float_val(metadata, "put_call_score"),
        float(len(metadata.get("sentiment_sources") or [])),
        _float_val(metadata, "whale_buy_vol"),
        _float_val(metadata, "whale_sell_vol"),
        _dominant_to_numeric(metadata.get("whale_dominant") or metadata.get("whale_data")),
        _bool_val(metadata, "whale_data"),
        _bool_val(metadata, "orderflow_applicable"),
        _float_val(metadata, "orderflow_imbalance"),
        _float_val(metadata, "liquidity_proxy"),
        _float_val(metadata, "spread_penalty"),
        _float_val(metadata, "seed_candidate_score", _float_val(metadata, "ml_confidence")),
        _float_val(metadata, "memory_score") / 100.0,
        _float_val(metadata, "memory_edge"),
        _float_val(metadata, "memory_win_rate"),
        _float_val(metadata, "memory_similarity"),
        min(1.0, _float_val(metadata, "memory_sample_count") / 50.0),
        _float_val(metadata, "opportunity_score"),
    ], dtype=np.float32)

    return np.concatenate([features, extra])


class TradingAgent:
    """Policy-only trading agent that generates and scores signals from a learned model."""

    def __init__(self):
        self._lock = threading.Lock()

    def _predict_policy(
        self,
        asset: str,
        category: str,
        df: pd.DataFrame,
        context: Dict[str, Any],
    ) -> Tuple[float, float, str]:
        state = build_agent_features(df, context)
        if state is None:
            return 0.5, 0.0, "features_unavailable"

        model_key = f"{category}_policy"
        model = registry.get(model_key)
        if model is None:
            logger.debug(f"[TradingAgent] No policy model found for {category}")
            return 0.5, 0.0, "model_unavailable"

        try:
            expected_features = getattr(model, "n_features_in_", None)
            if isinstance(expected_features, (int, np.integer)) and expected_features > 0 and expected_features != len(state):
                if expected_features < len(state):
                    logger.warning(
                        f"[TradingAgent] Policy model feature mismatch for {asset} ({model_key}): "
                        f"model expects {expected_features}, runtime built {len(state)} — policy gate bypassed pending retrain"
                    )
                    return 0.5, 0.0, "feature_mismatch"
                logger.warning(
                    f"[TradingAgent] Policy model feature mismatch for {asset} ({model_key}): "
                    f"model expects {expected_features}, runtime built {len(state)} — policy gate bypassed"
                )
                return 0.5, 0.0, "feature_mismatch"

            with self._lock:
                proba = model.predict_proba(state.reshape(1, -1))
            up_prob = float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
            confidence = squash_confidence(abs(up_prob - 0.5) * 2)
            logger.log_ml(model_key, asset, up_prob, confidence)
            return up_prob, confidence, "ok"
        except Exception as e:
            logger.warning(f"[TradingAgent] Policy scoring failed for {asset} ({model_key}): {e}")
            return 0.5, 0.0, "predict_failed"

    def score(self, asset: str, category: str, df: pd.DataFrame, context: Dict[str, Any]) -> Tuple[float, float]:
        up_prob, confidence, _status = self._predict_policy(asset, category, df, context)
        return up_prob, confidence

    def generate_signal(
        self,
        asset: str,
        canonical: str,
        category: str,
        df: pd.DataFrame,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Signal]:
        if context is None:
            context = {}

        if "signal_metadata" not in context:
            context["signal_metadata"] = {
                "ml_prediction_real": False,
                "seed_candidate_score": 0.0,
                "sentiment_score": context.get("sentiment_score", 0.0),
                "regime": context.get("regime", "unknown"),
            }

        from ml.features import build_features as _build_base_features
        features = _build_base_features(df)
        if features is None:
            return None

        context["features"] = features
        state = build_agent_features(df, context)
        if state is None:
            return None

        model_key = f"{category}_policy"
        model = registry.get(model_key)
        if model is None:
            logger.debug(f"[TradingAgent] No policy model available for {category}")
            return None

        try:
            with self._lock:
                proba = model.predict_proba(state.reshape(1, -1))
            up_prob = float(proba[0][1]) if proba.shape[1] > 1 else float(proba[0][0])
        except Exception as e:
            logger.debug(f"[TradingAgent] generate_signal failed for {asset}: {e}")
            return None

        confidence = squash_confidence(abs(up_prob - 0.5) * 2)
        if up_prob >= 0.55:
            direction = "BUY"
        elif up_prob <= 0.45:
            direction = "SELL"
        else:
            return None

        try:
            entry_price = float(df["close"].iloc[-1])
        except Exception:
            entry_price = 0.0
        if entry_price <= 0.0:
            return None

        risk_manager = context.get("risk_manager")
        if risk_manager is not None:
            stop_loss = risk_manager.get_stop_loss(entry_price, direction, category)
            take_profit = risk_manager.get_take_profit(entry_price, stop_loss, direction, rr=2.0)
        else:
            dist = entry_price * 0.015
            stop_loss = entry_price - dist if direction == "BUY" else entry_price + dist
            take_profit = entry_price + dist * 2 if direction == "BUY" else entry_price - dist * 2

        signal = Signal(
            asset=asset,
            canonical_asset=canonical,
            category=category,
            direction=direction,
            confidence=round(clamp_confidence(confidence), 4),
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_reward=0.0,
            strategy_id="policy_agent",
            indicators={"policy_source": model_key},
        )
        signal.metadata["agent_score"] = up_prob
        signal.metadata["agent_confidence"] = confidence
        signal.metadata["policy_model"] = model_key
        signal.metadata["signal_metadata"] = context.get("signal_metadata", {})
        return signal

    def decide(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        if signal is None:
            return None

        model_key = f"{signal.category}_policy"
        ctx = dict(context or {})
        merged_metadata = {
            **signal.metadata,
            "confidence": signal.confidence,
            "direction": signal.direction,
        }
        existing_metadata = _parse_metadata(ctx.get("signal_metadata"))
        ctx["signal_metadata"] = {
            **existing_metadata,
            **merged_metadata,
        }

        prob, conf, status = self._predict_policy(signal.asset, signal.category, ctx.get("price_data"), ctx)
        signal.metadata["policy_model"] = model_key
        signal.metadata["agent_score"] = prob
        signal.metadata["agent_confidence"] = conf
        signal.metadata["agent_policy_status"] = status

        if status != "ok":
            signal.metadata["agent_recommended_score"] = round(signal.confidence, 4)
            signal.metadata["agent_policy_advisory"] = f"policy gate bypassed ({status})"
            return signal

        if signal.direction == "BUY":
            directional_edge = prob
            passed = prob >= 0.55
            reject_reason = f"policy score {prob:.3f} below BUY threshold 0.55"
        elif signal.direction == "SELL":
            directional_edge = 1.0 - prob
            passed = prob <= 0.45
            reject_reason = f"policy score {prob:.3f} above SELL threshold 0.45"
        else:
            directional_edge = 0.5
            passed = False
            reject_reason = f"unsupported direction {signal.direction!r}"

        signal.metadata["agent_directional_edge"] = round(directional_edge, 4)

        # Final layer decides whether this signal has enough learned edge.
        if not passed:
            signal.metadata["agent_rejection_reason"] = reject_reason
            logger.debug(f"[TradingAgent] Rejected {signal.asset} {signal.direction} by {reject_reason}")
            return None

        agent_floor = 0.55 + abs(prob - 0.5) * 0.40
        signal.metadata["agent_recommended_score"] = round(clamp_confidence(agent_floor), 4)
        return signal


# ── singleton ──────────────────────────────────────────────────────────
agent = TradingAgent()
