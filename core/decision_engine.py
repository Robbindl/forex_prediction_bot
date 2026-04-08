from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np

from config.config import (
    MAX_SIGNAL_CONFIDENCE,
    MIN_CONFIDENCE_SCORE,
    MIN_FINAL_CONFIDENCE,
    PLAYBOOK_ONLY_RUNTIME,
    SPREAD_THRESHOLDS,
    get_trading_timeframe,
)
from core.asset_profiles import get_profile
from core.signal import Signal
from core.signal_journal import INFO, KILLED, PASS
from services.signal_intelligence import apply_cross_asset_review, apply_sentiment_review, apply_whale_review
from utils.logger import get_logger

try:
    from monitoring.metrics import DECISION, metrics
    from monitoring.system_health_service import monitor as _monitor
    _MONITOR_OK = True
except ImportError:
    _MONITOR_OK = False

logger = get_logger()

STEP_MARKET = 1
STEP_INTELLIGENCE = 2
STEP_EXECUTION = 3
STEP_POLICY = 4
STEP_GOVERNANCE = 5
_NYSE_FIXED_HOLIDAYS = frozenset({
    (1, 1),
    (5, 1),
    (7, 4),
    (12, 25),
    (12, 26),
})


def count_valid_sources(signal: Signal) -> int:
    count = 0
    if signal.metadata.get("ml_prediction_real", True):
        count += 1
    if signal.metadata.get("regime") not in (None, "unknown"):
        count += 1
    if signal.metadata.get("sentiment_sources", []):
        count += 1
    if signal.metadata.get("market_intelligence_sources", []):
        count += 1
    if signal.metadata.get("whale_data") == "real":
        count += 1
    if signal.metadata.get("meta_ai_active_engines", 0) > 0:
        count += 1
    if signal.metadata.get("orderflow_applicable") is True and signal.metadata.get("orderflow_imbalance", 0.0) != 0.0:
        count += 1
    return count


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _utc_hour() -> int:
    return _utc_now().hour


def _is_exchange_holiday() -> bool:
    now = _utc_now()
    md = (now.month, now.day)
    if md in _NYSE_FIXED_HOLIDAYS:
        return True
    if now.month == 11 and now.weekday() == 3 and 22 <= now.day <= 28:
        return True
    return False


def _active_session() -> str:
    now = _utc_now()
    hour = now.hour
    weekday = now.weekday()
    if weekday == 5 or weekday == 6:
        if weekday == 6 and hour >= 22:
            return "asia"
        return "off"
    if weekday == 4 and hour >= 22:
        return "off"
    if 0 <= hour < 6:
        return "asia"
    if 6 <= hour < 14:
        return "europe"
    if 14 <= hour < 22:
        return "us"
    return "off"


def _is_market_open(category: str) -> bool:
    hour = _utc_hour()
    weekday = _utc_now().weekday() < 5
    if not weekday:
        return category == "crypto"
    if category == "crypto":
        return True
    if category != "crypto" and _is_exchange_holiday():
        return False
    if category == "forex":
        return _active_session() != "off"
    if category in ("stocks", "indices"):
        return 13 <= hour < 21
    if category == "commodities":
        return hour != 21
    return True


def _market_status_for_signal(signal: Signal, context: Dict[str, Any]) -> tuple[bool, str]:
    market_status = context.get("market_status")
    if isinstance(market_status, dict) and "market_open" in market_status:
        return bool(market_status.get("market_open")), str(market_status.get("reason", "market status unavailable"))

    asset = str(signal.canonical_asset or signal.asset or "").strip()
    if asset:
        try:
            from services.market_data_router import get_market_status

            status = get_market_status(asset, category=signal.category)
            if status and "market_open" in status:
                return bool(status["market_open"]), str(status.get("reason", "market status"))
        except Exception:
            pass

    utc_hour = _utc_hour()
    if _is_market_open(signal.category):
        return True, "open"
    return False, f"market closed for {signal.category} at UTC {utc_hour:02d}:xx"


def _get_news_state(category: str) -> Dict[str, Any]:
    try:
        from data_ingestion.news_event_monitor import news_monitor
        return news_monitor.get_event_state(category)
    except Exception:
        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


def _detect_regime(df, timeframe: str = "15m") -> str:
    if df is None or len(df) < 30:
        return "unknown"
    try:
        close = df["close"].astype(float)
        sma20 = close.rolling(20).mean()
        sma50 = close.rolling(50).mean() if len(df) >= 50 else sma20
        returns = close.pct_change().dropna()
        bars_per_year = {"15m": 24192, "1h": 6048, "4h": 1512, "1d": 252}
        ann_factor = bars_per_year.get(str(timeframe or "15m").lower(), 252)
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
    try:
        from order_flow import get_imbalance
        symbol = asset.replace("-USD", "USDT").replace("/", "").replace("-", "")
        return get_imbalance(symbol)
    except Exception:
        return 0.0


class SignalDecisionEngine:
    def evaluate(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Optional[Signal]:
        context = context or {}
        context.setdefault("decision_start", time.monotonic())
        try:
            if not self._apply_market_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_intelligence_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_memory_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_policy_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_governance_review(signal, context):
                return self._finalize(signal, context)
            if not self._apply_execution_review(signal, context):
                return self._finalize(signal, context)
        except Exception as exc:
            logger.error(f"[DecisionEngine] Fatal evaluation error: {exc}", exc_info=True)
            if signal.alive:
                signal.kill(f"decision engine exception: {exc}", STEP_GOVERNANCE)
            signal.journal.record(
                layer=STEP_GOVERNANCE,
                name="governance",
                decision=KILLED,
                reason=f"exception: {exc}",
                conf_before=signal.confidence,
                conf_after=signal.confidence,
            )
        return self._finalize(signal, context)

    run = evaluate

    @staticmethod
    def _kill_review(
        signal: Signal,
        *,
        step: int,
        name: str,
        reason: str,
        conf_before: float,
        data: Dict[str, Any],
    ) -> bool:
        signal.kill(reason, step)
        signal.journal.record(
            layer=step,
            name=name,
            decision=KILLED,
            reason=reason,
            conf_before=conf_before,
            conf_after=signal.confidence,
            data=data,
        )
        return False

    @staticmethod
    def _market_seed_and_ml(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> float:
        seed_below_floor = signal.confidence < MIN_CONFIDENCE_SCORE
        signal.metadata["seed_below_floor"] = seed_below_floor
        data["seed_below_floor"] = seed_below_floor
        if seed_below_floor:
            notes.append(f"seed_conf<{MIN_CONFIDENCE_SCORE:.2f}")

        ml_pred = context.get("ml_prediction")
        ml_conf = float(context.get("ml_confidence", 0.0) or 0.0)
        signal.metadata["ml_confidence"] = round(ml_conf, 4)
        if ml_pred is not None and ml_conf > 0.1:
            signal.metadata["ml_prediction_real"] = True
            signal.metadata["ml_prediction"] = round(float(ml_pred), 4)
            ml_direction = "BUY" if ml_pred > 0.5 else "SELL"
            signal.metadata["ml_direction"] = ml_direction
            signal.metadata["ml_direction_agrees"] = ml_direction == signal.direction
            data["ml_direction"] = ml_direction
            data["ml_direction_agrees"] = ml_direction == signal.direction
            notes.append("ml_agrees" if ml_direction == signal.direction else "ml_disagrees")
        else:
            signal.metadata["ml_prediction_real"] = False
            notes.append("ml_unavailable")
        return ml_conf

    @staticmethod
    def _market_rr_and_spread(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> float:
        rr = 0.0
        if signal.entry_price and signal.stop_loss and signal.take_profit:
            try:
                risk = abs(signal.entry_price - signal.stop_loss)
                reward = abs(signal.take_profit - signal.entry_price)
                if risk > 0:
                    rr = reward / risk
                    signal.risk_reward = round(rr, 2)
                    if rr < 1.2:
                        notes.append("low_rr")
                    elif rr >= 3.0:
                        notes.append("excellent_rr")
            except Exception:
                pass
        data["rr"] = round(rr, 2)

        spread = context.get("spread")
        spread_pct = 0.0
        if spread and signal.entry_price and signal.entry_price > 0:
            try:
                spread_pct = float(spread) / float(signal.entry_price)
                if spread_pct > 0.005:
                    notes.append("spread_penalty")
            except Exception:
                pass
        signal.metadata["observed_spread_pct"] = round(spread_pct, 6)
        data["spread_pct"] = round(spread_pct, 5)
        return rr

    @staticmethod
    def _market_broker_quality(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> None:
        broker_quality = context.get("broker_quality") or {}
        if not isinstance(broker_quality, dict) or not broker_quality:
            return
        try:
            broker_score = float(broker_quality.get("score", 0.0) or 0.0)
            agreement_state = str(broker_quality.get("quote_agreement_state", "unconfirmed") or "unconfirmed")
            agreement_score = broker_quality.get("quote_agreement_score")
            spread_regime = str(broker_quality.get("spread_regime", "unknown") or "unknown")
            quote_quality_state = str(broker_quality.get("quote_quality_state", "unknown") or "unknown")
            market_state = str(broker_quality.get("market_state", "unknown") or "unknown")
            transition_risk = float(broker_quality.get("market_transition_risk", 0.0) or 0.0)
            fallback_active = bool(broker_quality.get("fallback_active"))

            signal.metadata["broker_quality"] = dict(broker_quality)
            signal.metadata["broker_quality_score"] = round(broker_score, 4)
            signal.metadata["broker_agreement_state"] = agreement_state
            signal.metadata["broker_spread_regime"] = spread_regime
            signal.metadata["broker_quote_quality_state"] = quote_quality_state
            signal.metadata["broker_market_state"] = market_state
            data["broker_quality"] = {
                "score": round(broker_score, 4),
                "primary_provider": broker_quality.get("primary_provider"),
                "comparison_provider": broker_quality.get("comparison_provider"),
                "agreement_state": agreement_state,
                "agreement_score": round(float(agreement_score), 4) if agreement_score is not None else None,
                "spread_regime": spread_regime,
                "quote_quality_state": quote_quality_state,
                "market_state": market_state,
                "market_state_transition": broker_quality.get("market_state_transition", ""),
                "market_transition_risk": round(transition_risk, 4),
                "fallback_active": fallback_active,
            }

            if agreement_state in {"strong", "aligned"}:
                notes.append("broker_confirmed")
            elif agreement_state == "divergent":
                notes.append("broker_divergence")
            elif agreement_state == "severe_divergence":
                notes.append("broker_severe_divergence")

            if spread_regime in {"wide", "stressed", "extreme"}:
                notes.append(f"spread_{spread_regime}")
            if quote_quality_state in {"aging", "stale", "delayed"}:
                notes.append(f"quote_{quote_quality_state}")
            if broker_quality.get("market_state_changed"):
                notes.append("market_state_changed")
            if transition_risk >= 0.65:
                notes.append("market_transition_risk")
            if fallback_active:
                notes.append("provider_fallback_active")
        except Exception:
            pass

    @staticmethod
    def _market_microstructure(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> None:
        micro = context.get("market_microstructure") or {}
        if not isinstance(micro, dict) or not micro:
            return
        try:
            micro_score = float(micro.get("score", 0.0) or 0.0)
            stop_hunt_risk = float(micro.get("stop_hunt_risk", 0.0) or 0.0)
            exhaustion_risk = float(micro.get("exhaustion_risk", 0.0) or 0.0)
            tick_imbalance = float(micro.get("tick_imbalance", 0.0) or 0.0)
            book_imbalance = float(micro.get("book_imbalance", 0.0) or 0.0)
            velocity_bps = float(micro.get("velocity_bps", 0.0) or 0.0)
            aligned_micro = micro_score if signal.direction == "BUY" else -micro_score
            aligned_book = book_imbalance if signal.direction == "BUY" else -book_imbalance
            aligned_tick = tick_imbalance if signal.direction == "BUY" else -tick_imbalance
            aligned_velocity = velocity_bps if signal.direction == "BUY" else -velocity_bps
            signal.metadata["market_microstructure"] = dict(micro)
            signal.metadata["microstructure_score"] = round(micro_score, 3)
            signal.metadata["stop_hunt_risk"] = round(stop_hunt_risk, 3)
            signal.metadata["exhaustion_risk"] = round(exhaustion_risk, 3)
            signal.metadata["microstructure_alignment"] = round(aligned_micro, 3)
            signal.metadata["tick_imbalance"] = round(tick_imbalance, 4)
            signal.metadata["book_imbalance"] = round(book_imbalance, 4)
            signal.metadata["velocity_bps"] = round(velocity_bps, 4)
            signal.metadata["depth_available"] = bool(micro.get("depth_available"))
            signal.metadata["synthetic_depth_available"] = bool(micro.get("synthetic_depth_available"))
            signal.metadata["microstructure_source"] = str(micro.get("microstructure_source") or "")
            data["microstructure_score"] = round(micro_score, 3)
            data["stop_hunt_risk"] = round(stop_hunt_risk, 3)
            data["exhaustion_risk"] = round(exhaustion_risk, 3)
            data["tick_imbalance"] = round(tick_imbalance, 4)
            data["book_imbalance"] = round(book_imbalance, 4)
            data["velocity_bps"] = round(velocity_bps, 4)
            data["synthetic_depth_available"] = bool(micro.get("synthetic_depth_available"))
            if stop_hunt_risk >= 0.45:
                notes.append("stop_hunt_penalty")
            if exhaustion_risk >= 0.42:
                notes.append("micro_exhaustion")
            if micro.get("synthetic_depth_available"):
                notes.append("synthetic_depth_proxy")
            if aligned_micro >= 0.20:
                notes.append("micro_boost")
            elif aligned_micro <= -0.20:
                notes.append("micro_penalty")
            if aligned_book >= 0.18:
                notes.append("book_pressure_support")
            elif aligned_book <= -0.18:
                notes.append("book_pressure_conflict")
            if aligned_tick >= 0.22 and aligned_velocity > 0:
                notes.append("micro_momentum_support")
            elif aligned_tick <= -0.22 and aligned_velocity < 0:
                notes.append("micro_momentum_conflict")
        except Exception:
            pass

    @staticmethod
    def _market_structure(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> Dict[str, Any]:
        structure = context.get("market_structure") or {}
        if not isinstance(structure, dict) or not structure:
            return {}

        structure_bias = str(structure.get("structure_bias", "neutral")).lower()
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        volatility_state = str(structure.get("volatility_state", "unknown"))
        distance_to_support = structure.get("distance_to_support")
        distance_to_resistance = structure.get("distance_to_resistance")

        signal.metadata["market_structure"] = dict(structure)
        signal.metadata["structure_bias"] = structure_bias
        signal.metadata["alignment_score"] = round(alignment_score, 4)
        signal.metadata["setup_quality"] = round(setup_quality, 4)
        signal.metadata["volatility_state"] = volatility_state

        direction_sign = 1 if signal.direction == "BUY" else -1
        dominant_setup = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score
        signal.metadata["dominant_setup_score"] = round(dominant_setup, 4)
        signal.metadata["setup_direction_alignment"] = round(dominant_setup * direction_sign, 4)
        data["market_structure"] = {
            "structure_bias": structure_bias,
            "alignment_score": round(alignment_score, 4),
            "setup_quality": round(setup_quality, 4),
            "pullback_score": round(pullback_score, 4),
            "breakout_score": round(breakout_score, 4),
            "volatility_state": volatility_state,
            "distance_to_support": distance_to_support,
            "distance_to_resistance": distance_to_resistance,
        }

        if structure_bias in {"buy", "sell"}:
            if (structure_bias == "buy" and signal.direction == "BUY") or (
                structure_bias == "sell" and signal.direction == "SELL"
            ):
                notes.append("structure_aligned")
            elif alignment_score >= 0.45:
                notes.append("structure_conflict")

        if dominant_setup * direction_sign >= 0.45:
            notes.append("setup_confirmed")
        elif dominant_setup * direction_sign <= -0.45:
            notes.append("setup_conflict")

        if setup_quality < 0.25:
            notes.append("weak_setup_quality")

        if volatility_state == "extreme":
            notes.append("extreme_volatility")
        elif volatility_state == "expansion" and dominant_setup * direction_sign >= 0.35:
            notes.append("volatility_support")

        try:
            if signal.direction == "BUY" and distance_to_resistance is not None and float(distance_to_resistance) <= 0.0025:
                notes.append("near_resistance")
            if signal.direction == "SELL" and distance_to_support is not None and float(distance_to_support) <= 0.0025:
                notes.append("near_support")
        except Exception:
            pass
        return structure

    @staticmethod
    def _market_regime(signal: Signal, context: Dict[str, Any], structure: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> str:
        profile = get_profile(signal.asset)
        timeframe = context.get("timeframe") or get_trading_timeframe(profile.category)
        df = context.get("price_data")
        regime = (
            str(structure.get("regime"))
            if isinstance(structure, dict) and structure.get("regime")
            else _detect_regime(df, timeframe=timeframe) if df is not None else context.get("regime", "unknown")
        )
        signal.metadata["regime"] = regime
        data["regime"] = regime

        imbalance = 0.0
        if profile.use_order_flow:
            imbalance = _get_orderflow_imbalance(signal.asset)
            signal.metadata["orderflow_applicable"] = True
        else:
            signal.metadata["orderflow_applicable"] = False
        signal.metadata["orderflow_imbalance"] = round(imbalance, 3)
        data["orderflow_imbalance"] = round(imbalance, 3)

        allowed = {"BUY": {"trending_up", "ranging", "unknown"}, "SELL": {"trending_down", "ranging", "unknown"}}.get(signal.direction, {"unknown"})
        if regime in ("trending_up", "trending_down") and (
            (signal.direction == "BUY" and regime == "trending_up")
            or (signal.direction == "SELL" and regime == "trending_down")
        ):
            notes.append("trend_aligned")
        if regime == "volatile":
            notes.append("volatile")
        elif regime not in allowed:
            notes.append("regime_conflict")

        if profile.use_order_flow:
            direction_sign = 1 if signal.direction == "BUY" else -1
            if imbalance * direction_sign > 0.30:
                notes.append("orderflow_support")
            elif imbalance * direction_sign < -0.30:
                notes.append("orderflow_conflict")
        return regime

    @staticmethod
    def _market_news(signal: Signal, data: Dict[str, Any], notes: List[str]) -> tuple[str, str, str, str, int]:
        news = _get_news_state(signal.category)
        news_state = str(news.get("state", "clear") or "clear")
        event_name = str(news.get("event", "") or "")
        impact = str(news.get("impact", "") or "")
        news_direction = str(news.get("direction", "") or "")
        mins = int(news.get("mins_to", 0) or 0)
        signal.metadata["news_state"] = news_state
        signal.metadata["news_event"] = event_name
        signal.metadata["news_impact"] = impact
        signal.metadata["news_direction"] = news_direction
        signal.metadata["news_mins_to"] = mins
        data["news_state"] = news_state
        data["news"] = {
            "event": event_name,
            "impact": impact,
            "direction": news_direction,
            "mins_to": mins,
        }
        if news_state == "pre" and impact == "MEDIUM":
            notes.append("medium_event_pre")
        if news_state == "post" and news_direction:
            if news_direction == signal.direction:
                signal.metadata["news_alignment"] = "aligned"
                notes.append("post_event_aligned")
            else:
                signal.metadata["news_alignment"] = "conflict"
                notes.append("post_event_conflict")
        return news_state, event_name, impact, news_direction, mins

    def _apply_market_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        data: Dict[str, Any] = {}
        notes: List[str] = []

        ml_conf = self._market_seed_and_ml(signal, context, data, notes)
        rr = self._market_rr_and_spread(signal, context, data, notes)
        self._market_broker_quality(signal, context, data, notes)
        self._market_microstructure(signal, context, data, notes)
        structure = self._market_structure(signal, context, data, notes)
        regime = self._market_regime(signal, context, structure, data, notes)

        session = _active_session()
        utc_hour = _utc_hour()
        signal.metadata["session"] = session
        data["session"] = session
        data["utc_hour"] = utc_hour

        market_open, market_reason = _market_status_for_signal(signal, context)
        data["market_open"] = bool(market_open)
        data["market_reason"] = market_reason

        if not market_open:
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=market_reason,
                conf_before=conf_before,
                data=data,
            )

        news_state, event_name, impact, _news_direction, mins = self._market_news(signal, data, notes)

        if news_state == "pre" and impact == "HIGH":
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=f"HIGH impact event in {mins}min: {event_name}",
                conf_before=conf_before,
                data=data,
            )

        if news_state == "active" and impact == "HIGH":
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=f"HIGH impact event active: {event_name}",
                conf_before=conf_before,
                data=data,
            )

        signal.metadata["market_review_notes"] = list(notes)
        data["notes"] = notes
        signal.step_reached = STEP_MARKET
        signal.journal.record(
            layer=STEP_MARKET,
            name="market",
            decision=PASS,
            reason=f"ml={ml_conf:.3f} rr={rr:.2f} regime={regime} session={session} news={news_state}",
            conf_before=conf_before,
            conf_after=signal.confidence,
            data=data,
        )
        return True

    def _apply_intelligence_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        sentiment = apply_sentiment_review(signal, context)
        whale = apply_whale_review(signal, context)
        cross_asset = apply_cross_asset_review(signal, context)
        signal.step_reached = STEP_INTELLIGENCE
        signal.journal.record(
            layer=STEP_INTELLIGENCE,
            name="intelligence",
            decision=PASS,
            reason=(
                f"sentiment={float(sentiment.get('score', 0.0)):+.3f} "
                f"whale={whale.get('dominant', 'n/a')} "
                f"cross={float(cross_asset.get('alignment', 0.0)):+.3f} "
                f"sources={len(sentiment.get('sources', []))}"
            ),
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "sentiment_score": sentiment.get("score"),
                "sentiment_sources": sentiment.get("sources", []),
                "narrative": sentiment.get("dominant_narrative", ""),
                "whale_dominant": whale.get("dominant"),
                "whale_ratio": whale.get("ratio"),
                "cross_asset_score": cross_asset.get("score"),
                "cross_asset_alignment": cross_asset.get("alignment"),
                "cross_asset_confidence": cross_asset.get("confidence"),
                "cross_asset_state": cross_asset.get("state"),
                "cross_asset_supportive_direction": cross_asset.get("supportive_direction"),
                "cross_asset_primary_peer": cross_asset.get("dominant_peer"),
                "cross_asset_primary_relation": cross_asset.get("dominant_relation"),
                "cross_asset_peers": cross_asset.get("peers", []),
                "market_intel_sources": signal.metadata.get("market_intelligence_sources", []),
            },
        )
        return True

    @staticmethod
    def _execution_adaptive_policy(
        signal: Signal,
        context: Dict[str, Any],
        data: Dict[str, Any],
        engine: Any,
        category: str,
    ) -> Dict[str, Any]:
        adaptive_policy: Dict[str, Any] = {}
        try:
            from services.adaptive_policy_service import get_service as get_adaptive_policy_service

            adaptive_policy = get_adaptive_policy_service().get_thresholds(
                asset=signal.asset,
                category=category,
                context=context,
                signal=signal,
                state=getattr(engine, "state", None) if engine else None,
            )
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Adaptive policy unavailable for {signal.asset}: {exc}")

        normalized = {
            "raw": adaptive_policy,
            "max_spread_pct": float(adaptive_policy.get("max_spread", SPREAD_THRESHOLDS.get(category, 0.002)) or 0.002),
            "min_final_conf": float(adaptive_policy.get("min_final_confidence", MIN_FINAL_CONFIDENCE) or MIN_FINAL_CONFIDENCE),
            "adaptive_risk_multiplier": float(adaptive_policy.get("risk_multiplier", 1.0) or 1.0),
            "adaptive_min_rr": float(adaptive_policy.get("min_rr", 0.0) or 0.0),
            "adaptive_target_rr_multiplier": float(adaptive_policy.get("target_rr_multiplier", 1.0) or 1.0),
            "adaptive_block": bool(adaptive_policy.get("block_new_entries")),
            "adaptive_block_reason": str(adaptive_policy.get("block_reason") or "").strip(),
        }
        if adaptive_policy:
            signal.metadata["adaptive_policy"] = dict(adaptive_policy)
            data["adaptive_policy"] = {
                "min_final_confidence": round(normalized["min_final_conf"], 4),
                "max_spread": round(normalized["max_spread_pct"], 6),
                "risk_multiplier": round(normalized["adaptive_risk_multiplier"], 4),
                "cooldown_minutes": int(adaptive_policy.get("cooldown_minutes", 0) or 0),
                "min_rr": round(normalized["adaptive_min_rr"], 2),
                "target_rr_multiplier": round(normalized["adaptive_target_rr_multiplier"], 4),
                "block_new_entries": normalized["adaptive_block"],
                "block_reason": normalized["adaptive_block_reason"],
                "recent_review_profile": dict(adaptive_policy.get("recent_review_profile") or {}),
                "notes": list(adaptive_policy.get("notes") or []),
            }
        return normalized

    @staticmethod
    def _execution_apply_target_rr(
        signal: Signal,
        adaptive_target_rr_multiplier: float,
        has_managed_target_plan: bool,
        data: Dict[str, Any],
    ) -> None:
        if not (
            signal.entry_price
            and signal.stop_loss
            and signal.take_profit
            and abs(adaptive_target_rr_multiplier - 1.0) > 1e-6
            and not has_managed_target_plan
        ):
            return
        try:
            risk = abs(float(signal.entry_price) - float(signal.stop_loss))
            current_reward = abs(float(signal.take_profit) - float(signal.entry_price))
            if risk <= 0 or current_reward <= 0:
                return
            current_rr = current_reward / risk
            adjusted_rr = max(1.0, current_rr * adaptive_target_rr_multiplier)
            adjusted_reward = risk * adjusted_rr
            if signal.direction == "BUY":
                signal.take_profit = round(float(signal.entry_price) + adjusted_reward, 6)
            else:
                signal.take_profit = round(float(signal.entry_price) - adjusted_reward, 6)
            signal.risk_reward = round(adjusted_rr, 2)
            signal.metadata["adaptive_target_rr_multiplier"] = round(adaptive_target_rr_multiplier, 4)
            data["adaptive_target_rr_applied"] = {
                "previous_rr": round(current_rr, 4),
                "target_rr_multiplier": round(adaptive_target_rr_multiplier, 4),
                "adjusted_rr": round(adjusted_rr, 4),
            }
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Adaptive target RR adjustment failed for {signal.asset}: {exc}")

    @staticmethod
    def _execution_sync_managed_targets(signal: Signal, staged_targets: List[float], data: Dict[str, Any]) -> None:
        if not staged_targets:
            return
        try:
            final_target = float(staged_targets[-1])
            signal.take_profit_levels = list(staged_targets)
            signal.take_profit = round(final_target, 6)
            risk = abs(float(signal.entry_price) - float(signal.stop_loss))
            if risk > 0:
                signal.risk_reward = round(abs(final_target - float(signal.entry_price)) / risk, 2)
            signal.metadata["primary_take_profit"] = round(float(staged_targets[0]), 6)
            signal.metadata["runner_take_profit"] = round(final_target, 6)
            data["trade_management_targets"] = {
                "primary_take_profit": round(float(staged_targets[0]), 6),
                "runner_take_profit": round(final_target, 6),
                "staged_target_count": len(staged_targets),
            }
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Managed target sync failed for {signal.asset}: {exc}")

    def _execution_spread_gate(
        self,
        signal: Signal,
        spread: Any,
        price: Any,
        max_spread_pct: float,
        conf_before: float,
        data: Dict[str, Any],
        notes: List[str],
    ) -> bool:
        if not (spread and price and price > 0):
            return True
        try:
            liquidity = float(spread) / float(price)
            data["liquidity_proxy"] = round(liquidity, 6)
            if liquidity > max_spread_pct:
                return self._kill_review(
                    signal,
                    step=STEP_EXECUTION,
                    name="execution",
                    reason=f"final spread {liquidity:.5f} > {max_spread_pct} ({signal.category})",
                    conf_before=conf_before,
                    data=data,
                )
            signal.metadata["liquidity_proxy"] = round(liquidity, 6)
            if liquidity > max_spread_pct * 0.75:
                notes.append("spread_heavy")
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Spread gate failed for {signal.asset}: {exc}")
        return True

    def _apply_execution_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        price = signal.entry_price
        spread = context.get("spread")
        category = context.get("category", signal.category or "forex")
        data: Dict[str, Any] = {}
        notes: List[str] = []
        engine = context.get("engine")
        management_plan = (
            signal.metadata.get("trade_management_plan")
            if isinstance(signal.metadata.get("trade_management_plan"), dict)
            else {}
        )
        staged_targets: List[float] = []
        for raw_level in list(getattr(signal, "take_profit_levels", []) or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                staged_targets.append(round(level, 6))
        has_managed_target_plan = bool(management_plan and staged_targets)
        adaptive_policy = self._execution_adaptive_policy(signal, context, data, engine, category)
        max_spread_pct = float(adaptive_policy["max_spread_pct"])
        min_final_conf = float(adaptive_policy["min_final_conf"])
        adaptive_risk_multiplier = float(adaptive_policy["adaptive_risk_multiplier"])
        adaptive_min_rr = float(adaptive_policy["adaptive_min_rr"])
        adaptive_target_rr_multiplier = float(adaptive_policy["adaptive_target_rr_multiplier"])
        adaptive_block = bool(adaptive_policy["adaptive_block"])
        adaptive_block_reason = str(adaptive_policy["adaptive_block_reason"])

        if adaptive_block:
            return self._kill_review(
                signal,
                step=STEP_EXECUTION,
                name="execution",
                reason=adaptive_block_reason or "recent similar setups are blocked by post-trade learning",
                conf_before=conf_before,
                data=data,
            )

        self._execution_apply_target_rr(signal, adaptive_target_rr_multiplier, has_managed_target_plan, data)

        df = context.get("price_data")
        if df is not None and len(df) >= 20:
            try:
                high = df["high"].astype(float)
                low = df["low"].astype(float)
                recent_low = low.iloc[-20:].min()
                recent_high = high.iloc[-20:].max()
                entry_range = recent_high - recent_low
                current_range = high.iloc[-1] - low.iloc[-1]
                recent_avg_range = (high.iloc[-20:-1] - low.iloc[-20:-1]).mean()
                if recent_avg_range > 0:
                    volatility_ratio = current_range / recent_avg_range
                    signal.metadata["volatility_ratio"] = round(float(volatility_ratio), 4)
                    data["volatility_ratio"] = round(float(volatility_ratio), 3)
                    if volatility_ratio < 0.60:
                        notes.append("compressed_volatility")
                    elif volatility_ratio > 1.40:
                        notes.append("expanded_volatility")
                if entry_range > 0:
                    if signal.direction == "BUY":
                        proximity = (signal.entry_price - recent_low) / entry_range
                        signal.metadata["support_proximity"] = round(float(proximity), 4)
                        data["support_proximity"] = round(float(proximity), 3)
                        if proximity < 0.15:
                            notes.append("buy_near_support")
                    else:
                        proximity = (recent_high - signal.entry_price) / entry_range
                        signal.metadata["resistance_proximity"] = round(float(proximity), 4)
                        data["resistance_proximity"] = round(float(proximity), 3)
                        if proximity < 0.15:
                            notes.append("sell_near_resistance")
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Entry quality check failed for {signal.asset}: {exc}")

        structure = context.get("market_structure") or signal.metadata.get("market_structure") or {}
        align_tp_fn = getattr(getattr(engine, "_risk_manager", None), "align_take_profit_to_structure", None) if engine else None
        if (
            callable(align_tp_fn)
            and signal.entry_price
            and signal.stop_loss
            and signal.take_profit
            and not has_managed_target_plan
        ):
            try:
                aligned_tp = align_tp_fn(
                    float(signal.entry_price),
                    float(signal.take_profit),
                    signal.direction,
                    category=category,
                    structure=structure if isinstance(structure, dict) else {},
                    atr=float(signal.metadata.get("atr", 0.0) or 0.0),
                    confidence=float(signal.confidence or 0.0),
                )
                if isinstance(aligned_tp, (int, float)) and aligned_tp > 0:
                    aligned_tp = float(aligned_tp)
                    previous_tp = float(signal.take_profit)
                    if abs(aligned_tp - previous_tp) > 1e-9:
                        risk = abs(float(signal.entry_price) - float(signal.stop_loss))
                        adjusted_rr = abs(aligned_tp - float(signal.entry_price)) / risk if risk > 0 else float(signal.risk_reward or 0.0)
                        signal.take_profit = round(aligned_tp, 6)
                        signal.risk_reward = round(adjusted_rr, 2)
                        structure_alignment = {
                            "base_take_profit": round(previous_tp, 6),
                            "aligned_take_profit": round(aligned_tp, 6),
                            "adjusted_rr": round(adjusted_rr, 4),
                            "regime": str((structure or {}).get("regime") or ""),
                            "structure_bias": str((structure or {}).get("structure_bias") or ""),
                        }
                        signal.metadata["structure_target_alignment"] = structure_alignment
                        data["structure_target_alignment"] = structure_alignment
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Structure target alignment failed for {signal.asset}: {exc}")

        if has_managed_target_plan:
            self._execution_sync_managed_targets(signal, staged_targets, data)

        if not self._execution_spread_gate(signal, spread, price, max_spread_pct, conf_before, data, notes):
            return False

        if adaptive_min_rr > 0 and float(signal.risk_reward or 0.0) < adaptive_min_rr:
            rr_gap = max(0.0, adaptive_min_rr - float(signal.risk_reward or 0.0))
            signal.metadata["adaptive_rr_gap"] = round(rr_gap, 4)
            data["adaptive_rr_gap"] = round(rr_gap, 4)
            notes.append("rr_below_policy")

        signal.metadata["execution_review_notes"] = list(notes)
        data["notes"] = list(notes)

        try:
            from services.signal_scorecard import get_service as get_signal_scorecard_service

            scorecard = get_signal_scorecard_service().score(signal, context)
            signal.confidence = float(scorecard.get("final_score", signal.confidence) or signal.confidence)
            signal.metadata["scorecard"] = scorecard
            signal.metadata["live_validation_profile"] = dict(scorecard.get("live_validation") or {})
            data["scorecard"] = {
                "raw_score": scorecard.get("raw_score"),
                "reliability": scorecard.get("reliability"),
                "breakdown": dict(scorecard.get("breakdown") or {}),
                "notes": list(scorecard.get("notes") or []),
            }
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Signal scorecard unavailable for {signal.asset}: {exc}")

        if signal.confidence <= min_final_conf:
            reason = f"final score {signal.confidence:.3f} below floor {min_final_conf:.3f}"
            signal.kill(reason, STEP_EXECUTION)
            signal.journal.record(
                layer=STEP_EXECUTION,
                name="execution",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

        if engine and getattr(engine, "_risk_manager", None):
            try:
                sizing_confidence = min(MAX_SIGNAL_CONFIDENCE, max(MIN_CONFIDENCE_SCORE, signal.confidence * adaptive_risk_multiplier))
                size = engine._risk_manager.calculate_position_size(
                    entry_price=signal.entry_price,
                    stop_loss=signal.stop_loss,
                    category=signal.category,
                    confidence=sizing_confidence,
                    asset=signal.asset,
                )
                signal.position_size = size
                signal.risk_parameters["position_size"] = size
                signal.risk_parameters["adaptive_risk_multiplier"] = round(adaptive_risk_multiplier, 4)
                data["position_size"] = round(size, 6)
                data["sizing_confidence"] = round(sizing_confidence, 4)
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Position sizing failed for {signal.asset}: {exc}")

        if signal.entry_price and signal.take_profit and not signal.take_profit_levels:
            try:
                entry = signal.entry_price
                tp1 = signal.take_profit
                dist = abs(tp1 - entry)
                if dist > 0:
                    if signal.direction == "BUY":
                        signal.take_profit_levels = [round(entry + dist * 0.5, 6), round(entry + dist, 6), round(entry + dist * 1.5, 6)]
                    else:
                        signal.take_profit_levels = [round(entry - dist * 0.5, 6), round(entry - dist, 6), round(entry - dist * 1.5, 6)]
            except Exception as exc:
                logger.debug(f"[DecisionEngine] TP level calculation failed for {signal.asset}: {exc}")

        signal.step_reached = STEP_EXECUTION
        signal.journal.record(
            layer=STEP_EXECUTION,
            name="execution",
            decision=PASS,
            reason=f"final_score={signal.confidence:.3f} size={signal.position_size:.4f} tp_levels={len(signal.take_profit_levels)}",
            conf_before=conf_before,
            conf_after=signal.confidence,
            data=data,
        )
        return True

    def _apply_memory_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        try:
            from services.setup_memory_service import get_service as get_setup_memory_service

            memory = get_setup_memory_service().score_setup(signal, context)
        except Exception as exc:
            signal.journal.record(
                layer=0,
                name="memory",
                decision=INFO,
                reason=f"setup memory unavailable: {exc}",
                conf_before=conf_before,
                conf_after=signal.confidence,
            )
            return True

        fingerprint = memory.get("fingerprint") or {}
        signal.metadata["setup_memory_fingerprint"] = fingerprint
        signal.metadata["setup_memory"] = memory
        signal.metadata["memory_score"] = memory.get("memory_score")
        signal.metadata["memory_edge"] = memory.get("memory_edge")
        signal.metadata["memory_win_rate"] = memory.get("win_rate")
        signal.metadata["memory_similarity"] = memory.get("avg_similarity")
        signal.metadata["memory_sample_count"] = memory.get("sample_count")

        adjustment = float(memory.get("adjustment", 0.0) or 0.0)
        reason = (
            f"memory score={float(memory.get('memory_score', 50.0)):.1f} "
            f"edge={float(memory.get('memory_edge', 0.0)):+.3f} "
            f"samples={int(memory.get('sample_count', 0) or 0)}"
        )
        signal.journal.record(
            layer=0,
            name="memory",
            decision=INFO,
            reason=reason,
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "memory_score": memory.get("memory_score"),
                "memory_edge": memory.get("memory_edge"),
                "memory_win_rate": memory.get("win_rate"),
                "memory_similarity": memory.get("avg_similarity"),
                "memory_sample_count": memory.get("sample_count"),
                "same_asset_matches": memory.get("same_asset_matches"),
                "adjustment": adjustment,
                "notes": memory.get("notes", []),
                "fingerprint": fingerprint,
            },
        )
        return True

    def _apply_policy_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        policy_status = "playbook_only" if PLAYBOOK_ONLY_RUNTIME else "policy_retired"
        advisory = (
            "policy review skipped in playbook-only runtime"
            if PLAYBOOK_ONLY_RUNTIME
            else "legacy policy review removed; playbook path is primary"
        )
        signal.metadata["agent_policy_status"] = policy_status
        signal.metadata["agent_policy_advisory"] = advisory
        signal.metadata["policy_review_passed"] = True
        signal.step_reached = STEP_POLICY
        signal.journal.record(
            layer=STEP_POLICY,
            name="policy",
            decision=PASS,
            reason=advisory,
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "agent_policy_status": policy_status,
                "final_confidence": round(signal.confidence, 4),
            },
        )
        return True

    def _apply_governance_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        profile = get_profile(signal.asset)
        valid_sources = count_valid_sources(signal)
        min_required = profile.min_valid_layers
        signal.metadata["valid_sources_count"] = valid_sources
        signal.metadata["min_sources_required"] = min_required
        data = {"valid_sources": valid_sources, "min_required": min_required, "category": signal.category}

        adaptive_policy_preview: Dict[str, Any] = {}
        try:
            from services.adaptive_policy_service import get_service as get_adaptive_policy_service

            adaptive_policy_preview = get_adaptive_policy_service().get_thresholds(
                asset=signal.asset,
                category=signal.category,
                context=context,
                signal=signal,
                state=getattr(context.get("engine"), "state", None) if context.get("engine") else None,
            )
            if adaptive_policy_preview:
                context["adaptive_policy"] = dict(adaptive_policy_preview)
                signal.metadata["adaptive_policy"] = dict(adaptive_policy_preview)
                data["adaptive_policy_preview"] = {
                    "min_rr": round(float(adaptive_policy_preview.get("min_rr", 0.0) or 0.0), 2),
                    "min_final_confidence": round(float(adaptive_policy_preview.get("min_final_confidence", 0.0) or 0.0), 4),
                    "block_new_entries": bool(adaptive_policy_preview.get("block_new_entries")),
                }
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Adaptive policy preview unavailable for {signal.asset}: {exc}")

        if valid_sources < min_required:
            reason = f"Insufficient real data: {valid_sources}/{min_required} sources for {signal.asset} ({signal.category})"
            signal.kill(reason, STEP_GOVERNANCE)
            signal.journal.record(
                layer=STEP_GOVERNANCE,
                name="governance",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

        try:
            try:
                from services.signal_scorecard import get_service as get_signal_scorecard_service

                provisional_scorecard = get_signal_scorecard_service().score(signal, context)
                signal.confidence = float(provisional_scorecard.get("final_score", signal.confidence) or signal.confidence)
                signal.metadata["scorecard"] = provisional_scorecard
                data["scorecard_preview"] = {
                    "raw_score": provisional_scorecard.get("raw_score"),
                    "reliability": provisional_scorecard.get("reliability"),
                }
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Signal scorecard preview unavailable for {signal.asset}: {exc}")

            from services.signal_governance import signal_governance
            verdict = signal_governance.evaluate(signal, context)
            signal.metadata["governance_validation"] = verdict
            data.update({
                "grade": verdict.get("grade"),
                "score": verdict.get("score"),
                "model_key": verdict.get("model_key"),
                "live_validation": verdict.get("live_validation"),
            })
            if not verdict.get("approved"):
                reason = verdict.get("reason", "signal governance rejected signal")
                signal.kill(reason, STEP_GOVERNANCE)
                signal.journal.record(
                    layer=STEP_GOVERNANCE,
                    name="governance",
                    decision=KILLED,
                    reason=reason,
                    conf_before=conf_before,
                    conf_after=signal.confidence,
                    data={**data, "violations": verdict.get("violations", [])},
                )
                return False

            signal.step_reached = STEP_GOVERNANCE
            signal.journal.record(
                layer=STEP_GOVERNANCE,
                name="governance",
                decision=PASS,
                reason=f"grade={verdict.get('grade', 'n/a')} score={verdict.get('score', 0)}",
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return True
        except Exception as exc:
            reason = f"signal governance exception: {exc}"
            signal.kill(reason, STEP_GOVERNANCE)
            signal.journal.record(
                layer=STEP_GOVERNANCE,
                name="governance",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

    def _finalize(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        elapsed_ms = (time.monotonic() - context.get("decision_start", time.monotonic())) * 1000

        if os.getenv("DEBUG_FORCE_SURVIVE", "0") == "1" and not signal.alive:
            signal.alive = True
            signal.kill_reason = "Forced survive via DEBUG_FORCE_SURVIVE"

        if signal.alive:
            logger.info(f"[DecisionEngine] {signal.asset} accepted score={signal.confidence:.3f} ({elapsed_ms:.0f}ms)")
        else:
            logger.debug(f"[DecisionEngine] {signal.asset} rejected at step {signal.step_reached}: {signal.kill_reason} ({elapsed_ms:.0f}ms)")

        if _MONITOR_OK:
            try:
                metrics.record(DECISION, elapsed_ms, success=signal.alive)
                _monitor.record_decision_latency(elapsed_ms)
                _monitor.record_signal(signal.asset, signal.direction, signal.alive)
                if not signal.alive and signal.kill_reason:
                    _monitor.record_kill(str(signal.step_reached))
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Monitoring record failed: {exc}")

        if signal.alive:
            try:
                from prediction_tracker import prediction_tracker as _pt
                _pt.record_signal({
                    "asset": signal.asset,
                    "direction": signal.direction,
                    "signal": signal.direction,
                    "entry_price": signal.entry_price,
                    "take_profit": signal.take_profit,
                    "stop_loss": signal.stop_loss,
                    "confidence": signal.confidence,
                    "category": signal.category,
                    "strategy": signal.strategy_id,
                    "session": signal.metadata.get("session", ""),
                    "regime": signal.metadata.get("regime", ""),
                    "features": context.get("features"),
                    "signal_metadata": {
                        "ml_prediction": context.get("ml_prediction"),
                        "ml_confidence": context.get("ml_confidence"),
                        **signal.metadata,
                    },
                })
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Prediction tracker record failed: {exc}")

        try:
            from core.signal_reporter import reporter
            signal = reporter.report(signal, context)
        except Exception as exc:
            logger.error(f"[DecisionEngine] Reporter error: {exc}")

        return signal if signal.alive else None


decision_engine = SignalDecisionEngine()
