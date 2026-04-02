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
    SPREAD_THRESHOLDS,
    get_trading_timeframe,
)
from core.asset_profiles import get_profile
from core.signal import Signal
from core.signal_journal import INFO, KILLED, PASS
from services.signal_intelligence import apply_sentiment_review, apply_whale_review
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
            from services.deriv_bridge import deriv_bridge

            status = deriv_bridge.get_market_status(asset, category=signal.category)
            if status and "market_open" in status:
                return bool(status["market_open"]), str(status.get("reason", "Deriv market status"))
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

    def _apply_market_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        data: Dict[str, Any] = {}
        notes: List[str] = []

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

        micro = context.get("market_microstructure") or {}
        if isinstance(micro, dict) and micro:
            try:
                micro_score = float(micro.get("score", 0.0) or 0.0)
                stop_hunt_risk = float(micro.get("stop_hunt_risk", 0.0) or 0.0)
                aligned_micro = micro_score if signal.direction == "BUY" else -micro_score
                signal.metadata["market_microstructure"] = dict(micro)
                signal.metadata["microstructure_score"] = round(micro_score, 3)
                signal.metadata["stop_hunt_risk"] = round(stop_hunt_risk, 3)
                signal.metadata["microstructure_alignment"] = round(aligned_micro, 3)
                data["microstructure_score"] = round(micro_score, 3)
                data["stop_hunt_risk"] = round(stop_hunt_risk, 3)
                if stop_hunt_risk >= 0.45:
                    notes.append("stop_hunt_penalty")
                if aligned_micro >= 0.20:
                    notes.append("micro_boost")
                elif aligned_micro <= -0.20:
                    notes.append("micro_penalty")
            except Exception:
                pass

        structure = context.get("market_structure") or {}
        if isinstance(structure, dict) and structure:
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
            structure_data = {
                "structure_bias": structure_bias,
                "alignment_score": round(alignment_score, 4),
                "setup_quality": round(setup_quality, 4),
                "pullback_score": round(pullback_score, 4),
                "breakout_score": round(breakout_score, 4),
                "volatility_state": volatility_state,
                "distance_to_support": distance_to_support,
                "distance_to_resistance": distance_to_resistance,
            }
            data["market_structure"] = structure_data

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
        if regime in ("trending_up", "trending_down"):
            if (signal.direction == "BUY" and regime == "trending_up") or (signal.direction == "SELL" and regime == "trending_down"):
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

        session = _active_session()
        utc_hour = _utc_hour()
        signal.metadata["session"] = session
        data["session"] = session
        data["utc_hour"] = utc_hour

        market_open, market_reason = _market_status_for_signal(signal, context)
        data["market_open"] = bool(market_open)
        data["market_reason"] = market_reason

        if not market_open:
            reason = market_reason
            signal.kill(reason, STEP_MARKET)
            signal.journal.record(
                layer=STEP_MARKET,
                name="market",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

        news = _get_news_state(signal.category)
        news_state = news.get("state", "clear")
        event_name = news.get("event", "")
        impact = news.get("impact", "")
        direction = news.get("direction", "")
        mins = news.get("mins_to", 0)
        signal.metadata["news_state"] = news_state
        signal.metadata["news_event"] = event_name
        signal.metadata["news_impact"] = impact
        signal.metadata["news_direction"] = direction
        signal.metadata["news_mins_to"] = mins
        data["news_state"] = news_state
        data["news"] = {
            "event": event_name,
            "impact": impact,
            "direction": direction,
            "mins_to": mins,
        }

        if news_state == "pre" and impact == "HIGH":
            reason = f"HIGH impact event in {mins}min: {event_name}"
            signal.kill(reason, STEP_MARKET)
            signal.journal.record(
                layer=STEP_MARKET,
                name="market",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

        if news_state == "active" and impact == "HIGH":
            reason = f"HIGH impact event active: {event_name}"
            signal.kill(reason, STEP_MARKET)
            signal.journal.record(
                layer=STEP_MARKET,
                name="market",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data=data,
            )
            return False

        if news_state == "pre" and impact == "MEDIUM":
            notes.append("medium_event_pre")
        if news_state == "post" and direction:
            if direction == signal.direction:
                signal.metadata["news_alignment"] = "aligned"
                notes.append("post_event_aligned")
            else:
                signal.metadata["news_alignment"] = "conflict"
                notes.append("post_event_conflict")

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
        signal.step_reached = STEP_INTELLIGENCE
        signal.journal.record(
            layer=STEP_INTELLIGENCE,
            name="intelligence",
            decision=PASS,
            reason=f"sentiment={float(sentiment.get('score', 0.0)):+.3f} whale={whale.get('dominant', 'n/a')} sources={len(sentiment.get('sources', []))}",
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "sentiment_score": sentiment.get("score"),
                "sentiment_sources": sentiment.get("sources", []),
                "narrative": sentiment.get("dominant_narrative", ""),
                "whale_dominant": whale.get("dominant"),
                "whale_ratio": whale.get("ratio"),
                "market_intel_sources": signal.metadata.get("market_intelligence_sources", []),
            },
        )
        return True

    def _apply_execution_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        price = signal.entry_price
        spread = context.get("spread")
        category = context.get("category", signal.category or "forex")
        data: Dict[str, Any] = {}
        notes: List[str] = []
        engine = context.get("engine")
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

        max_spread_pct = float(adaptive_policy.get("max_spread", SPREAD_THRESHOLDS.get(category, 0.002)) or 0.002)
        min_final_conf = float(adaptive_policy.get("min_final_confidence", MIN_FINAL_CONFIDENCE) or MIN_FINAL_CONFIDENCE)
        adaptive_risk_multiplier = float(adaptive_policy.get("risk_multiplier", 1.0) or 1.0)
        adaptive_min_rr = float(adaptive_policy.get("min_rr", 0.0) or 0.0)
        if adaptive_policy:
            signal.metadata["adaptive_policy"] = dict(adaptive_policy)
            data["adaptive_policy"] = {
                "min_final_confidence": round(min_final_conf, 4),
                "max_spread": round(max_spread_pct, 6),
                "risk_multiplier": round(adaptive_risk_multiplier, 4),
                "cooldown_minutes": int(adaptive_policy.get("cooldown_minutes", 0) or 0),
                "min_rr": round(adaptive_min_rr, 2),
                "notes": list(adaptive_policy.get("notes") or []),
            }

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

        if spread and price and price > 0:
            try:
                liquidity = float(spread) / float(price)
                data["liquidity_proxy"] = round(liquidity, 6)
                if liquidity > max_spread_pct:
                    reason = f"final spread {liquidity:.5f} > {max_spread_pct} ({category})"
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
                signal.metadata["liquidity_proxy"] = round(liquidity, 6)
                if liquidity > max_spread_pct * 0.75:
                    notes.append("spread_heavy")
            except Exception as exc:
                logger.debug(f"[DecisionEngine] Spread gate failed for {signal.asset}: {exc}")

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
        try:
            from ml.meta_model import predictor
            signal = predictor.process(signal, context)
        except Exception as exc:
            logger.warning(f"[DecisionEngine] Meta AI unavailable for {signal.asset}: {exc}")
            signal.journal.record(
                layer=STEP_POLICY,
                name="meta_ai",
                decision=INFO,
                reason=f"meta AI unavailable: {exc}",
                conf_before=signal.confidence,
                conf_after=signal.confidence,
            )

        try:
            from ml.agent import agent as _agent
            policy_context = dict(context or {})
            policy_context["signal_metadata"] = {
                **signal.metadata,
                "seed_candidate_score": signal.metadata.get("seed_candidate_score", signal.confidence),
                "direction": signal.direction,
            }
            result = _agent.decide(signal, policy_context)
            if result is None:
                reason = signal.metadata.get("agent_rejection_reason", "Agent rejected signal")
                signal.kill(reason, STEP_POLICY)
                signal.journal.record(
                    layer=STEP_POLICY,
                    name="policy",
                    decision=KILLED,
                    reason=reason,
                    conf_before=conf_before,
                    conf_after=signal.confidence,
                    data={
                        "agent_score": round(float(signal.metadata.get("agent_score", 0.0)), 4),
                        "agent_confidence": round(float(signal.metadata.get("agent_confidence", 0.0)), 4),
                        "agent_directional_edge": round(float(signal.metadata.get("agent_directional_edge", 0.0)), 4),
                    },
                )
                return False
            policy_status = str(signal.metadata.get("agent_policy_status", "ok") or "ok")
            signal.metadata["policy_review_passed"] = True
            signal.step_reached = STEP_POLICY
            if policy_status == "ok":
                reason = f"policy accepted {signal.direction} (score={float(signal.metadata.get('agent_score', 0.0)):.3f})"
            else:
                reason = f"policy bypassed ({policy_status})"
            signal.journal.record(
                layer=STEP_POLICY,
                name="policy",
                decision=PASS,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data={
                    "agent_score": round(float(signal.metadata.get("agent_score", 0.0)), 4),
                    "agent_confidence": round(float(signal.metadata.get("agent_confidence", 0.0)), 4),
                    "agent_directional_edge": round(float(signal.metadata.get("agent_directional_edge", 0.0)), 4),
                    "agent_policy_status": policy_status,
                    "final_confidence": round(signal.confidence, 4),
                },
            )
            return True
        except Exception as exc:
            logger.error(f"[DecisionEngine] Policy agent error: {exc}")
            signal.step_reached = STEP_POLICY
            signal.journal.record(
                layer=STEP_POLICY,
                name="policy",
                decision=INFO,
                reason=f"policy agent unavailable: {exc}",
                conf_before=conf_before,
                conf_after=signal.confidence,
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
