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

_POLICY_REVERSAL_MIN_EDGE = 0.68
_POLICY_REVERSAL_MIN_ADVANTAGE = 0.18
_POLICY_REVERSAL_BASE_RR = {
    "forex": 1.5,
    "crypto": 1.7,
    "commodities": 1.6,
    "indices": 1.65,
    "stocks": 1.6,
}


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

    @staticmethod
    def _policy_research_state(model_key: str) -> tuple[bool, str]:
        meta = registry.get_metadata(model_key) if model_key else {}
        if bool(meta.get("research_approved")):
            return True, "approved"

        research_state = str(
            meta.get("research_status")
            or meta.get("research_grade")
            or "unapproved"
        ).strip().lower() or "unapproved"
        return False, research_state

    @staticmethod
    def _signal_model_key(signal: Signal, category: str) -> str:
        return str(
            signal.metadata.get("seed_model")
            or signal.metadata.get("model_key")
            or f"{category}_classifier"
        )

    @staticmethod
    def _default_reversal_rr(category: str) -> float:
        return float(_POLICY_REVERSAL_BASE_RR.get(str(category or "").lower(), 1.5))

    @staticmethod
    def _build_take_profit_levels(entry: float, take_profit: float, direction: str) -> list[float]:
        try:
            dist = abs(float(take_profit) - float(entry))
            if dist <= 0.0:
                return []
            if str(direction or "").upper() == "BUY":
                return [
                    round(entry + dist * 0.5, 6),
                    round(entry + dist, 6),
                    round(entry + dist * 1.5, 6),
                ]
            return [
                round(entry - dist * 0.5, 6),
                round(entry - dist, 6),
                round(entry - dist * 1.5, 6),
            ]
        except Exception:
            return []

    @staticmethod
    def _reprice_for_direction(signal: Signal, direction: str, context: Dict[str, Any]) -> None:
        try:
            entry = float(signal.entry_price or 0.0)
        except Exception:
            entry = 0.0
        if entry <= 0.0:
            return

        risk_manager = context.get("risk_manager")
        if risk_manager is None:
            engine = context.get("engine")
            risk_manager = getattr(engine, "_risk_manager", None) if engine is not None else None

        try:
            atr = float(signal.metadata.get("atr", 0.0) or 0.0)
        except Exception:
            atr = 0.0

        rr = 0.0
        try:
            rr = float(signal.risk_reward or 0.0)
        except Exception:
            rr = 0.0

        baseline_rr = TradingAgent._default_reversal_rr(signal.category)

        if risk_manager is not None:
            try:
                stop_loss = risk_manager.get_stop_loss(entry, direction, signal.category, atr=atr)
                target_rr = max(rr, float(risk_manager.get_target_rr(signal.category) or baseline_rr)) if rr > 0 else float(risk_manager.get_target_rr(signal.category) or baseline_rr)
                take_profit = risk_manager.get_take_profit(
                    entry,
                    stop_loss,
                    direction,
                    signal.category,
                    rr=target_rr,
                )
                signal.stop_loss = stop_loss
                signal.take_profit = take_profit
                signal.risk_reward = round(float(target_rr or 0.0), 2)
                signal.take_profit_levels = TradingAgent._build_take_profit_levels(entry, take_profit, direction)
                return
            except Exception as exc:
                logger.debug(f"[TradingAgent] Policy reversal repricing fallback for {signal.asset}: {exc}")

        risk_dist = abs(float(signal.stop_loss or 0.0) - entry)
        if risk_dist <= 0.0:
            risk_dist = entry * 0.015
        target_rr = max(rr, baseline_rr) if rr > 0 else baseline_rr
        if direction == "BUY":
            signal.stop_loss = entry - risk_dist
            signal.take_profit = entry + risk_dist * target_rr
        else:
            signal.stop_loss = entry + risk_dist
            signal.take_profit = entry - risk_dist * target_rr
        signal.risk_reward = round(float(target_rr or 0.0), 2)
        signal.take_profit_levels = TradingAgent._build_take_profit_levels(entry, float(signal.take_profit or 0.0), direction)

    def _maybe_reverse_signal(
        self,
        signal: Signal,
        *,
        prob: float,
        directional_edge: float,
        model_key: str,
        context: Dict[str, Any],
    ) -> bool:
        research_approved, _research_state = self._policy_research_state(model_key)
        if not research_approved:
            return False

        seed_model_key = self._signal_model_key(signal, signal.category)
        seed_meta = registry.get_metadata(seed_model_key) if seed_model_key else {}
        if bool(seed_meta.get("research_approved")):
            return False

        current_direction = str(signal.direction or "").upper()
        if current_direction == "BUY":
            opposite_direction = "SELL"
            opposite_edge = 1.0 - prob
        elif current_direction == "SELL":
            opposite_direction = "BUY"
            opposite_edge = prob
        else:
            return False

        if opposite_edge < _POLICY_REVERSAL_MIN_EDGE:
            return False
        if (opposite_edge - directional_edge) < _POLICY_REVERSAL_MIN_ADVANTAGE:
            return False

        signal.direction = opposite_direction
        if getattr(signal, "journal", None) is not None:
            signal.journal.direction = opposite_direction
        self._reprice_for_direction(signal, opposite_direction, context)
        signal.metadata["agent_policy_status"] = "reversed"
        signal.metadata["agent_policy_reversal_from"] = current_direction
        signal.metadata["agent_policy_reversal_to"] = opposite_direction
        signal.metadata["agent_policy_advisory"] = (
            f"policy reversed seed direction ({current_direction}->{opposite_direction}) "
            f"because approved policy outranked provisional seed"
        )
        signal.metadata["agent_recommended_score"] = round(
            clamp_confidence(0.55 + abs(prob - 0.5) * 0.40),
            4,
        )
        return True

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

        research_approved, research_state = self._policy_research_state(model_key)
        if not research_approved:
            bypass_status = "research_unapproved"
            signal.metadata["agent_policy_status"] = bypass_status
            signal.metadata["agent_recommended_score"] = round(signal.confidence, 4)
            signal.metadata["agent_policy_advisory"] = (
                f"policy gate bypassed (model research {research_state})"
            )
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
            if self._maybe_reverse_signal(
                signal,
                prob=prob,
                directional_edge=directional_edge,
                model_key=model_key,
                context=ctx,
            ):
                return signal
            signal.metadata["agent_rejection_reason"] = reject_reason
            logger.debug(f"[TradingAgent] Rejected {signal.asset} {signal.direction} by {reject_reason}")
            return None

        agent_floor = 0.55 + abs(prob - 0.5) * 0.40
        signal.metadata["agent_recommended_score"] = round(clamp_confidence(agent_floor), 4)
        return signal


# ── singleton ──────────────────────────────────────────────────────────
agent = TradingAgent()
