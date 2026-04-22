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

_SOURCE_FAMILY_FRESHNESS_SECS: Dict[str, int] = {
    "model": 3600,
    "regime": 3600,
    "sentiment": 1800,
    "macro": 3600,
    "positioning": 8 * 24 * 3600,
    "options": 3 * 24 * 3600,
    "flow": 300,
    "derivatives": 600,
    "cross_asset": 900,
}

_MARKET_INTELLIGENCE_FAMILY_MAP: Dict[str, str] = {
    "fred": "macro",
    "eia": "macro",
    "macro": "macro",
    "dxy": "macro",
    "rates": "macro",
    "real_yield": "macro",
    "yield_curve": "macro",
    "risk_regime": "macro",
    "cftc": "positioning",
    "aaii": "positioning",
    "client_sentiment": "positioning",
    "ig_client_sentiment": "positioning",
    "put_call": "options",
    "gamma": "options",
    "options": "options",
    "orderflow": "flow",
    "flow": "flow",
    "microstructure": "flow",
    "funding": "derivatives",
    "oi": "derivatives",
    "open_interest": "derivatives",
    "whale": "positioning",
    "onchain": "positioning",
}

_SENTIMENT_COMPONENT_FAMILY_MAP: Dict[str, str] = {
    "news": "sentiment",
    "reddit": "sentiment",
    "fear_greed": "sentiment",
    "vix": "sentiment",
    "price_momentum": "sentiment",
    "ig_client_sentiment": "positioning",
    "macro_event": "macro",
    "aaii": "positioning",
    "put_call": "options",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except Exception:
        return default


def _predictor_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    meta = metadata if isinstance(metadata, dict) else {}
    prediction = meta.get("predictor_prediction", meta.get("ml_prediction"))
    confidence = _safe_float(meta.get("predictor_confidence", meta.get("ml_confidence", 0.0)), 0.0)
    real_flag = meta.get("predictor_real")
    if real_flag is None:
        real_flag = meta.get("ml_prediction_real")
    real = bool(real_flag) if real_flag is not None else bool(prediction is not None and confidence > 0.10)
    direction = str(meta.get("predictor_direction", meta.get("ml_direction", "")) or "").strip().upper()
    direction_agrees = meta.get("predictor_direction_agrees")
    if direction_agrees is None:
        direction_agrees = meta.get("ml_direction_agrees")
    return {
        "prediction": prediction,
        "confidence": confidence if real else 0.0,
        "real": real,
        "direction": direction,
        "direction_agrees": bool(direction_agrees) if direction_agrees is not None else None,
        "model": str(meta.get("predictor_model", meta.get("ml_model", "")) or "").strip(),
        "provider": str(meta.get("predictor_provider", meta.get("ml_provider", "")) or "").strip(),
    }


def _allowed_source_families(signal: Signal) -> List[str]:
    families = list(get_profile(signal.asset).source_families or ())
    predictor = _predictor_metadata(signal.metadata if isinstance(signal.metadata, dict) else {})
    if not predictor["real"]:
        families = [family for family in families if family != "model"]
    return families


def _parse_optional_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 1_000_000_000_000:
            raw /= 1000.0
        try:
            return datetime.fromtimestamp(raw, timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _source_age_seconds(*values: Any) -> Optional[float]:
    ages: List[float] = []
    now = _utc_now()
    for value in values:
        parsed = _parse_optional_datetime(value)
        if parsed is None:
            continue
        ages.append(max(0.0, (now - parsed).total_seconds()))
    if not ages:
        return None
    return min(ages)


def _monitor_source_fresh(source: str) -> Optional[bool]:
    if not _MONITOR_OK:
        return None
    try:
        return bool(_monitor.is_source_fresh(source))
    except Exception:
        return None


def _family_is_fresh(family: str, *timestamps: Any) -> tuple[bool, Optional[float]]:
    age_secs = _source_age_seconds(*timestamps)

    if family == "flow":
        monitor_state = _monitor_source_fresh("order_book")
        if monitor_state is not None:
            return monitor_state, age_secs

    if family == "derivatives":
        monitor_states = [state for state in (_monitor_source_fresh("funding_rate"), _monitor_source_fresh("open_interest")) if state is not None]
        if monitor_states:
            return any(monitor_states), age_secs

    threshold = _SOURCE_FAMILY_FRESHNESS_SECS.get(family)
    if threshold is None or age_secs is None:
        return True, age_secs
    return age_secs <= threshold, age_secs


def _register_source_family(
    *,
    family: str,
    entries: List[str],
    timestamps: List[Any],
    allowed: set[str],
    valid: set[str],
    stale: set[str],
    evidence: Dict[str, List[str]],
    freshness: Dict[str, Dict[str, Any]],
) -> None:
    if family not in allowed:
        return
    cleaned = sorted({str(entry).strip() for entry in entries if str(entry).strip()})
    if not cleaned:
        return
    is_fresh, age_secs = _family_is_fresh(family, *timestamps)
    evidence[family] = cleaned
    freshness[family] = {
        "fresh": bool(is_fresh),
        "age_secs": round(age_secs, 1) if age_secs is not None else None,
        "threshold_secs": _SOURCE_FAMILY_FRESHNESS_SECS.get(family),
    }
    if is_fresh:
        valid.add(family)
    else:
        stale.add(family)


def _resolve_source_families(signal: Signal) -> tuple[List[str], List[str], Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
    profile = get_profile(signal.asset)
    metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
    allowed = set(_allowed_source_families(signal))
    valid: set[str] = set()
    stale: set[str] = set()
    evidence: Dict[str, List[str]] = {}
    freshness: Dict[str, Dict[str, Any]] = {}

    sentiment_sources = [str(item) for item in (metadata.get("sentiment_sources") or []) if str(item).strip()]
    sentiment_components = metadata.get("sentiment_components") if isinstance(metadata.get("sentiment_components"), dict) else {}
    market_intelligence_sources = [str(item) for item in (metadata.get("market_intelligence_sources") or []) if str(item).strip()]
    market_intelligence_details = metadata.get("market_intelligence_details") if isinstance(metadata.get("market_intelligence_details"), dict) else {}
    market_intelligence_components = metadata.get("market_intelligence_components") if isinstance(metadata.get("market_intelligence_components"), dict) else {}
    market_microstructure = metadata.get("market_microstructure") if isinstance(metadata.get("market_microstructure"), dict) else {}

    predictor = _predictor_metadata(metadata)
    if predictor["real"]:
        _register_source_family(
            family="model",
            entries=[
                "predictor_real",
                *( [f"model:{predictor['model']}"] if predictor["model"] else [] ),
                *( [f"provider:{predictor['provider']}"] if predictor["provider"] else [] ),
            ],
            timestamps=[signal.timestamp],
            allowed=allowed,
            valid=valid,
            stale=stale,
            evidence=evidence,
            freshness=freshness,
        )

    regime = str(metadata.get("regime") or "").strip().lower()
    if regime and regime != "unknown":
        _register_source_family(
            family="regime",
            entries=[f"regime:{regime}"],
            timestamps=[signal.timestamp],
            allowed=allowed,
            valid=valid,
            stale=stale,
            evidence=evidence,
            freshness=freshness,
        )

    sentiment_entries: List[str] = [f"source:{src}" for src in sentiment_sources]
    for component_name, family_name in _SENTIMENT_COMPONENT_FAMILY_MAP.items():
        if family_name == "sentiment" and component_name in sentiment_components:
            sentiment_entries.append(f"component:{component_name}")
    if metadata.get("sentiment_score") is not None:
        sentiment_entries.append("score:sentiment")
    _register_source_family(
        family="sentiment",
        entries=sentiment_entries,
        timestamps=[metadata.get("sentiment_timestamp")],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    macro_entries: List[str] = []
    positioning_entries: List[str] = []
    options_entries: List[str] = []
    for src in market_intelligence_sources:
        family_name = _MARKET_INTELLIGENCE_FAMILY_MAP.get(src.strip().lower())
        if family_name == "macro":
            macro_entries.append(f"source:{src}")
        elif family_name == "positioning":
            positioning_entries.append(f"source:{src}")
        elif family_name == "options":
            options_entries.append(f"source:{src}")

    if "macro_event" in sentiment_components:
        macro_entries.append("component:macro_event")
    if "macro" in market_intelligence_details:
        macro_entries.append("detail:macro")
    if "eia" in market_intelligence_details:
        macro_entries.append("detail:eia")
    for key in ("usd_macro", "risk_regime", "yield_curve", "real_yield", "eia_inventory"):
        if key in market_intelligence_components:
            macro_entries.append(f"component:{key}")
    _register_source_family(
        family="macro",
        entries=macro_entries,
        timestamps=[metadata.get("market_intelligence_timestamp"), metadata.get("sentiment_timestamp")],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    if "cftc" in market_intelligence_details:
        positioning_entries.append("detail:cftc")
    if "cftc_positioning" in market_intelligence_components:
        positioning_entries.append("component:cftc_positioning")
    if "aaii" in sentiment_components:
        positioning_entries.append("component:aaii")
    if isinstance(metadata.get("ig_client_sentiment"), dict) and metadata.get("ig_client_sentiment"):
        positioning_entries.append("component:ig_client_sentiment")
    if metadata.get("whale_data") == "real":
        positioning_entries.append("signal:whale_data")
    _register_source_family(
        family="positioning",
        entries=positioning_entries,
        timestamps=[
            metadata.get("market_intelligence_timestamp"),
            metadata.get("sentiment_timestamp"),
            metadata.get("whale_timestamp"),
            metadata.get("intelligence_timestamp"),
        ],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    if "put_call" in sentiment_components:
        options_entries.append("component:put_call")
    if metadata.get("put_call_score") is not None:
        options_entries.append("signal:put_call_score")
    _register_source_family(
        family="options",
        entries=options_entries,
        timestamps=[metadata.get("sentiment_timestamp"), metadata.get("market_intelligence_timestamp")],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    flow_entries: List[str] = []
    if metadata.get("orderflow_applicable") is True and abs(_safe_float(metadata.get("orderflow_imbalance"), 0.0)) > 0.0:
        flow_entries.append("signal:orderflow_imbalance")
    tick_imbalance = _safe_float(market_microstructure.get("tick_imbalance", metadata.get("tick_imbalance")), 0.0)
    book_imbalance = _safe_float(market_microstructure.get("book_imbalance", metadata.get("book_imbalance")), 0.0)
    velocity_bps = _safe_float(market_microstructure.get("velocity_bps", metadata.get("velocity_bps")), 0.0)
    if abs(tick_imbalance) >= 0.01:
        flow_entries.append("micro:tick_imbalance")
    if abs(book_imbalance) >= 0.01:
        flow_entries.append("micro:book_imbalance")
    if abs(velocity_bps) >= 0.05:
        flow_entries.append("micro:velocity_bps")
    if bool(metadata.get("depth_available", market_microstructure.get("depth_available"))):
        flow_entries.append("micro:true_depth")
    elif bool(metadata.get("synthetic_depth_available", market_microstructure.get("synthetic_depth_available"))):
        flow_entries.append("micro:synthetic_depth")
    _register_source_family(
        family="flow",
        entries=flow_entries,
        timestamps=[signal.timestamp],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    derivative_entries: List[str] = []
    funding_bias = str(metadata.get("funding_bias") or "").strip().upper()
    oi_signal = str(metadata.get("oi_signal") or "").strip().upper()
    if funding_bias and funding_bias not in {"NEUTRAL", "UNKNOWN", "N/A"}:
        derivative_entries.append(f"funding:{funding_bias.lower()}")
    if oi_signal and oi_signal not in {"NEUTRAL", "UNKNOWN", "N/A"}:
        derivative_entries.append(f"oi:{oi_signal.lower()}")
    _register_source_family(
        family="derivatives",
        entries=derivative_entries,
        timestamps=[metadata.get("derivatives_timestamp"), metadata.get("intelligence_timestamp")],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    cross_asset = metadata.get("cross_asset_context") if isinstance(metadata.get("cross_asset_context"), dict) else {}
    cross_alignment = _safe_float(metadata.get("cross_asset_alignment", cross_asset.get("alignment", cross_asset.get("score"))), 0.0)
    cross_confidence = _safe_float(metadata.get("cross_asset_confidence", cross_asset.get("confidence")), 0.0)
    cross_state = str(metadata.get("cross_asset_state", cross_asset.get("state", "")) or "").strip().lower()
    cross_peer = str(metadata.get("cross_asset_primary_peer", cross_asset.get("dominant_peer", "")) or "").strip()
    cross_relation = str(metadata.get("cross_asset_primary_relation", cross_asset.get("dominant_relation", "")) or "").strip()
    raw_cross_peers = metadata.get("cross_asset_peers", cross_asset.get("peers"))
    peer_count = int(metadata.get("cross_asset_peer_count", len(raw_cross_peers or [])) or 0)
    cross_entries: List[str] = []
    if cross_peer:
        cross_entries.append(f"peer:{cross_peer.lower()}")
    if cross_relation:
        cross_entries.append(f"relation:{cross_relation.lower()}")
    if cross_state:
        cross_entries.append(f"state:{cross_state}")
    if abs(cross_alignment) >= 0.12:
        cross_entries.append("signal:alignment")
    if cross_confidence >= 0.18:
        cross_entries.append("signal:confidence")
    if peer_count > 0:
        cross_entries.append(f"peers:{peer_count}")
    _register_source_family(
        family="cross_asset",
        entries=cross_entries,
        timestamps=[metadata.get("intelligence_timestamp"), signal.timestamp],
        allowed=allowed,
        valid=valid,
        stale=stale,
        evidence=evidence,
        freshness=freshness,
    )

    return sorted(valid), sorted(stale), evidence, freshness


def count_valid_sources(signal: Signal) -> int:
    valid_families, stale_families, evidence, freshness = _resolve_source_families(signal)
    signal.metadata["eligible_source_families"] = _allowed_source_families(signal)
    signal.metadata["valid_source_families"] = valid_families
    signal.metadata["stale_source_families"] = stale_families
    signal.metadata["source_family_evidence"] = evidence
    signal.metadata["source_family_freshness"] = freshness
    return len(valid_families)


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
    if weekday in (5, 6):
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


def _is_market_open(asset: str, category: str) -> bool:
    try:
        from services.market_hours_guard import build_market_status

        status = build_market_status(asset, category)
        if status and "market_open" in status:
            return bool(status["market_open"])
    except Exception:
        pass

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
                try:
                    from services.market_hours_guard import build_market_status

                    normalized = build_market_status(asset, signal.category, provider_status=status)
                    return bool(normalized["market_open"]), str(normalized.get("reason", "market status"))
                except Exception:
                    return bool(status["market_open"]), str(status.get("reason", "market status"))
        except Exception:
            pass

    try:
        from services.market_hours_guard import build_market_status

        fallback_status = build_market_status(asset, signal.category)
        return bool(fallback_status.get("market_open")), str(fallback_status.get("reason", "market status unavailable"))
    except Exception:
        pass

    utc_hour = _utc_hour()
    if _is_market_open(asset, signal.category):
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
    def _market_seed_and_predictor(signal: Signal, context: Dict[str, Any], data: Dict[str, Any], notes: List[str]) -> float:
        seed_below_floor = signal.confidence < MIN_CONFIDENCE_SCORE
        signal.metadata["seed_below_floor"] = seed_below_floor
        data["seed_below_floor"] = seed_below_floor
        if seed_below_floor:
            notes.append(f"seed_conf<{MIN_CONFIDENCE_SCORE:.2f}")

        predictor = _predictor_metadata(context)
        signal.metadata["predictor_confidence"] = round(float(predictor["confidence"]), 4)
        if predictor["real"] and predictor["prediction"] is not None:
            signal.metadata["predictor_real"] = True
            signal.metadata["predictor_prediction"] = round(float(predictor["prediction"]), 4)
            predictor_direction = predictor["direction"] or ("BUY" if float(predictor["prediction"]) > 0.5 else "SELL")
            predictor_agrees = predictor_direction == signal.direction
            signal.metadata["predictor_direction"] = predictor_direction
            signal.metadata["predictor_direction_agrees"] = predictor_agrees
            signal.metadata["ml_prediction"] = round(float(predictor["prediction"]), 4)
            signal.metadata["ml_confidence"] = round(float(predictor["confidence"]), 4)
            signal.metadata["ml_prediction_real"] = True
            signal.metadata["ml_direction"] = predictor_direction
            signal.metadata["ml_direction_agrees"] = predictor_agrees
            if predictor["model"]:
                signal.metadata["predictor_model"] = predictor["model"]
            if predictor["provider"]:
                signal.metadata["predictor_provider"] = predictor["provider"]
            data["predictor_direction"] = predictor_direction
            data["predictor_direction_agrees"] = predictor_agrees
            notes.append("predictor_agrees" if predictor_agrees else "predictor_disagrees")
        else:
            signal.metadata["predictor_real"] = False
            signal.metadata["ml_prediction_real"] = False
            notes.append("predictor_unavailable")
        return float(predictor["confidence"])

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
        vwap_distance_atr = float(structure.get("vwap_distance_atr", 0.0) or 0.0)
        session_quality_score = float(structure.get("session_quality_score", 0.0) or 0.0)
        candle_quality_score = float(structure.get("candle_quality_score", 0.0) or 0.0)
        extension_score = float(structure.get("extension_score", 0.0) or 0.0)
        target_efficiency_score = float(structure.get("target_efficiency_score", 0.0) or 0.0)
        impulse_age_bars = int(structure.get("impulse_age_bars", 0) or 0)
        breakout_retest_ready = bool(structure.get("breakout_retest_ready"))
        first_pullback_ready = bool(structure.get("first_pullback_ready"))
        liquidity_sweep_buy = bool(structure.get("liquidity_sweep_buy"))
        liquidity_sweep_sell = bool(structure.get("liquidity_sweep_sell"))
        failed_opposite_move_confirmed = bool(structure.get("failed_opposite_move_confirmed"))
        entry_confirmation_bars_required = int(structure.get("entry_confirmation_bars_required", 0) or 0)
        entry_confirmation_count = int(structure.get("entry_confirmation_count", 0) or 0)
        entry_confirmation_ready = bool(structure.get("entry_confirmation_ready"))
        pattern_family = str(structure.get("pattern_family", "unknown") or "unknown")
        elite_pattern_rank = float(structure.get("elite_pattern_rank", 0.0) or 0.0)
        cluster_penalty = float(structure.get("cluster_penalty", 0.0) or 0.0)
        regime_entry_policy = (
            dict(structure.get("regime_entry_policy"))
            if isinstance(structure.get("regime_entry_policy"), dict)
            else {}
        )

        signal.metadata["market_structure"] = dict(structure)
        signal.metadata["structure_bias"] = structure_bias
        signal.metadata["alignment_score"] = round(alignment_score, 4)
        signal.metadata["setup_quality"] = round(setup_quality, 4)
        signal.metadata["volatility_state"] = volatility_state
        signal.metadata["vwap_distance_atr"] = round(vwap_distance_atr, 4)
        signal.metadata["session_quality_score"] = round(session_quality_score, 4)
        signal.metadata["candle_quality_score"] = round(candle_quality_score, 4)
        signal.metadata["extension_score"] = round(extension_score, 4)
        signal.metadata["target_efficiency_score"] = round(target_efficiency_score, 4)
        signal.metadata["impulse_age_bars"] = int(impulse_age_bars)
        signal.metadata["breakout_retest_ready"] = breakout_retest_ready
        signal.metadata["first_pullback_ready"] = first_pullback_ready
        signal.metadata["liquidity_sweep_buy"] = liquidity_sweep_buy
        signal.metadata["liquidity_sweep_sell"] = liquidity_sweep_sell
        signal.metadata["failed_opposite_move_confirmed"] = failed_opposite_move_confirmed
        signal.metadata["entry_confirmation_bars_required"] = int(entry_confirmation_bars_required)
        signal.metadata["entry_confirmation_count"] = int(entry_confirmation_count)
        signal.metadata["entry_confirmation_ready"] = entry_confirmation_ready
        signal.metadata["pattern_family"] = pattern_family
        signal.metadata["elite_pattern_rank"] = round(elite_pattern_rank, 4)
        signal.metadata["cluster_penalty"] = round(cluster_penalty, 4)
        signal.metadata["regime_entry_policy"] = regime_entry_policy

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
            "vwap_distance_atr": round(vwap_distance_atr, 4),
            "session_quality_score": round(session_quality_score, 4),
            "candle_quality_score": round(candle_quality_score, 4),
            "extension_score": round(extension_score, 4),
            "target_efficiency_score": round(target_efficiency_score, 4),
            "impulse_age_bars": int(impulse_age_bars),
            "breakout_retest_ready": breakout_retest_ready,
            "first_pullback_ready": first_pullback_ready,
            "liquidity_sweep_buy": liquidity_sweep_buy,
            "liquidity_sweep_sell": liquidity_sweep_sell,
            "failed_opposite_move_confirmed": failed_opposite_move_confirmed,
            "entry_confirmation_bars_required": int(entry_confirmation_bars_required),
            "entry_confirmation_count": int(entry_confirmation_count),
            "entry_confirmation_ready": entry_confirmation_ready,
            "pattern_family": pattern_family,
            "elite_pattern_rank": round(elite_pattern_rank, 4),
            "cluster_penalty": round(cluster_penalty, 4),
            "regime_entry_policy": regime_entry_policy,
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
        if extension_score >= 1.10 or abs(vwap_distance_atr) >= 1.40:
            notes.append("extended_from_value")
        if candle_quality_score <= 0.32:
            notes.append("weak_trigger_candle")
        if session_quality_score <= 0.40:
            notes.append("weak_session_quality")
        if target_efficiency_score <= 0.30:
            notes.append("thin_target_path")
        if impulse_age_bars >= 5:
            notes.append("late_after_impulse")
        if failed_opposite_move_confirmed:
            notes.append("failed_opposite_reclaim")
        if entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
            notes.append("needs_entry_confirmation")
        if elite_pattern_rank >= 0.75:
            notes.append("elite_pattern_rank")
        elif elite_pattern_rank <= 0.22 and pattern_family != "unknown":
            notes.append("weak_pattern_rank")
        if cluster_penalty >= 0.18:
            notes.append("trade_cluster_risk")

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

        predictor_conf = self._market_seed_and_predictor(signal, context, data, notes)
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
            reason=f"predictor={predictor_conf:.3f} rr={rr:.2f} regime={regime} session={session} news={news_state}",
            conf_before=conf_before,
            conf_after=signal.confidence,
            data=data,
        )
        return True

    @staticmethod
    def _apply_intelligence_review(signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        sentiment = apply_sentiment_review(signal, context)
        whale = apply_whale_review(signal, context)
        cross_asset = apply_cross_asset_review(signal, context)
        intelligence = context.get("market_intelligence") if isinstance(context.get("market_intelligence"), dict) else {}
        intelligence_timestamp = str(intelligence.get("intelligence_timestamp") or signal.metadata.get("intelligence_timestamp") or "")
        funding_bias = str(context.get("funding_bias", intelligence.get("funding_bias", "NEUTRAL")) or "NEUTRAL")
        oi_signal = str(context.get("oi_signal", intelligence.get("oi_signal", "NEUTRAL")) or "NEUTRAL")
        signal.metadata["funding_bias"] = funding_bias
        signal.metadata["oi_signal"] = oi_signal
        if intelligence_timestamp:
            signal.metadata["intelligence_timestamp"] = intelligence_timestamp
            signal.metadata["derivatives_timestamp"] = intelligence_timestamp
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

    @staticmethod
    def _execution_staged_targets(signal: Signal) -> List[float]:
        staged_targets: List[float] = []
        for raw_level in list(getattr(signal, "take_profit_levels", []) or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                staged_targets.append(round(level, 6))
        return staged_targets

    @staticmethod
    def _execution_entry_quality(
        signal: Signal,
        df: Any,
        structure: Dict[str, Any],
        data: Dict[str, Any],
        notes: List[str],
    ) -> None:
        if df is None or len(df) < 20:
            return
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
            if entry_range <= 0:
                return
            support_distance = None
            resistance_distance = None
            try:
                support_distance = structure.get("distance_to_support") if isinstance(structure, dict) else None
                resistance_distance = structure.get("distance_to_resistance") if isinstance(structure, dict) else None
            except Exception:
                support_distance = None
                resistance_distance = None

            if signal.direction == "BUY":
                proximity = (signal.entry_price - recent_low) / entry_range
                signal.metadata["support_proximity"] = round(float(proximity), 4)
                data["support_proximity"] = round(float(proximity), 3)
                if proximity < 0.15:
                    notes.append("buy_near_support")
                if support_distance is not None:
                    try:
                        supportive_distance = float(support_distance)
                        signal.metadata["supportive_structure_distance"] = round(supportive_distance, 6)
                        data["supportive_structure_distance"] = round(supportive_distance, 6)
                    except Exception:
                        pass
            else:
                proximity = (recent_high - signal.entry_price) / entry_range
                signal.metadata["resistance_proximity"] = round(float(proximity), 4)
                data["resistance_proximity"] = round(float(proximity), 3)
                if proximity < 0.15:
                    notes.append("sell_near_resistance")
                if resistance_distance is not None:
                    try:
                        supportive_distance = float(resistance_distance)
                        signal.metadata["supportive_structure_distance"] = round(supportive_distance, 6)
                        data["supportive_structure_distance"] = round(supportive_distance, 6)
                    except Exception:
                        pass
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Entry quality check failed for {signal.asset}: {exc}")

    def _execution_late_entry_risk_gate(
        self,
        signal: Signal,
        *,
        adaptive_policy: Dict[str, Any],
        conf_before: float,
        structure: Dict[str, Any],
        data: Dict[str, Any],
        notes: List[str],
    ) -> bool:
        support_proximity = signal.metadata.get("support_proximity")
        resistance_proximity = signal.metadata.get("resistance_proximity")
        volatility_ratio = float(signal.metadata.get("volatility_ratio", 0.0) or 0.0)
        exhaustion_risk = float(signal.metadata.get("exhaustion_risk", 0.0) or 0.0)
        dominant_exhaustion = float(structure.get("dominant_exhaustion_score", 0.0) or 0.0)
        bias_exhausted = bool(structure.get("bias_exhausted"))
        alignment_score = float(structure.get("alignment_score", signal.metadata.get("alignment_score", 0.0)) or 0.0)
        setup_quality = float(structure.get("setup_quality", signal.metadata.get("setup_quality", 0.0)) or 0.0)
        vwap_distance_atr = float(structure.get("vwap_distance_atr", signal.metadata.get("vwap_distance_atr", 0.0)) or 0.0)
        session_quality_score = float(structure.get("session_quality_score", signal.metadata.get("session_quality_score", 0.0)) or 0.0)
        candle_quality_score = float(structure.get("candle_quality_score", signal.metadata.get("candle_quality_score", 0.0)) or 0.0)
        extension_score = float(structure.get("extension_score", signal.metadata.get("extension_score", 0.0)) or 0.0)
        target_efficiency_score = float(structure.get("target_efficiency_score", signal.metadata.get("target_efficiency_score", 0.0)) or 0.0)
        impulse_age_bars = int(structure.get("impulse_age_bars", signal.metadata.get("impulse_age_bars", 0)) or 0)
        breakout_retest_ready = bool(structure.get("breakout_retest_ready", signal.metadata.get("breakout_retest_ready")))
        first_pullback_ready = bool(structure.get("first_pullback_ready", signal.metadata.get("first_pullback_ready")))
        liquidity_sweep_buy = bool(structure.get("liquidity_sweep_buy", signal.metadata.get("liquidity_sweep_buy")))
        liquidity_sweep_sell = bool(structure.get("liquidity_sweep_sell", signal.metadata.get("liquidity_sweep_sell")))
        raw_policy = adaptive_policy.get("raw") if isinstance(adaptive_policy, dict) else {}
        recent_review = (
            raw_policy.get("recent_review_profile")
            if isinstance(raw_policy, dict) and isinstance(raw_policy.get("recent_review_profile"), dict)
            else {}
        )
        inactivity_profile = (
            raw_policy.get("inactivity_profile")
            if isinstance(raw_policy, dict) and isinstance(raw_policy.get("inactivity_profile"), dict)
            else {}
        )
        late_entry_rate = float(recent_review.get("late_entry_rate", 0.0) or 0.0)
        hard_loss_rate = float(recent_review.get("hard_loss_rate", 0.0) or 0.0)
        avg_rr_realized = float(recent_review.get("avg_rr_realized", 0.0) or 0.0)
        avg_quality_score = float(recent_review.get("avg_quality_score", 50.0) or 50.0)
        blocked_recent_pattern = bool(recent_review.get("block_new_entries"))
        blocked_recent_pattern_reason = str(recent_review.get("block_reason") or "").strip()
        confirmation_needed = bool(recent_review.get("confirmation_needed"))
        inactivity_relief_strength = float(inactivity_profile.get("relief_strength", 0.0) or 0.0)
        inactivity_relief_active = bool(inactivity_profile.get("active")) and inactivity_relief_strength > 0.0
        inactivity_flat_book = bool(inactivity_profile.get("flat_book"))
        inactivity_hours_since_last_entry = float(inactivity_profile.get("hours_since_last_entry", 0.0) or 0.0)
        elite_pattern_rank = max(
            float(recent_review.get("pattern_rank_score", 0.0) or 0.0),
            float(structure.get("elite_pattern_rank", signal.metadata.get("elite_pattern_rank", 0.0)) or 0.0),
        )
        cluster_penalty = max(
            float(recent_review.get("trade_cluster_penalty", 0.0) or 0.0),
            float(structure.get("cluster_penalty", signal.metadata.get("cluster_penalty", 0.0)) or 0.0),
        )
        failed_opposite_move_confirmed = bool(
            structure.get("failed_opposite_move_confirmed", signal.metadata.get("failed_opposite_move_confirmed"))
        )
        entry_confirmation_bars_required = int(
            structure.get("entry_confirmation_bars_required", signal.metadata.get("entry_confirmation_bars_required", 0)) or 0
        )
        entry_confirmation_count = int(
            structure.get("entry_confirmation_count", signal.metadata.get("entry_confirmation_count", 0)) or 0
        )
        entry_confirmation_ready = bool(
            structure.get("entry_confirmation_ready", signal.metadata.get("entry_confirmation_ready"))
        )
        pattern_family = str(structure.get("pattern_family", signal.metadata.get("pattern_family", "unknown")) or "unknown")
        regime_entry_policy = (
            dict(structure.get("regime_entry_policy"))
            if isinstance(structure.get("regime_entry_policy"), dict)
            else (
                dict(signal.metadata.get("regime_entry_policy"))
                if isinstance(signal.metadata.get("regime_entry_policy"), dict)
                else {}
            )
        )

        distance_to_resistance = structure.get("distance_to_resistance")
        distance_to_support = structure.get("distance_to_support")
        try:
            opposing_distance = (
                float(distance_to_resistance)
                if signal.direction == "BUY" and distance_to_resistance is not None
                else float(distance_to_support)
                if signal.direction == "SELL" and distance_to_support is not None
                else 1.0
            )
        except Exception:
            opposing_distance = 1.0

        try:
            directional_extension = (
                float(support_proximity)
                if signal.direction == "BUY" and support_proximity is not None
                else float(resistance_proximity)
                if signal.direction == "SELL" and resistance_proximity is not None
                else 0.0
            )
        except Exception:
            directional_extension = 0.0

        broker_agreement_state = str(signal.metadata.get("broker_agreement_state", "") or "").lower()
        broker_spread_regime = str(signal.metadata.get("broker_spread_regime", "") or "").lower()
        broker_quote_quality_state = str(signal.metadata.get("broker_quote_quality_state", "") or "").lower()
        synthetic_depth_only = bool(signal.metadata.get("synthetic_depth_available")) and not bool(signal.metadata.get("depth_available"))
        cross_asset_alignment = float(signal.metadata.get("cross_asset_alignment", 0.0) or 0.0)
        cross_asset_confidence = float(signal.metadata.get("cross_asset_confidence", 0.0) or 0.0)
        supportive_structure_distance = float(signal.metadata.get("supportive_structure_distance", 0.0) or 0.0)
        category_label = str(signal.category or signal.metadata.get("category") or "").strip().lower()
        strong_market_candidate = bool(
            category_label in {"crypto", "forex", "commodities", "indices"}
            and float(signal.confidence or 0.0) >= 0.64
            and alignment_score >= 0.68
            and setup_quality >= 0.62
            and candle_quality_score >= 0.36
            and session_quality_score >= 0.40
        )
        strong_fx_crypto_candidate = bool(
            category_label in {"crypto", "forex"} and strong_market_candidate
        )
        elite_supported_candidate = bool(
            strong_market_candidate
            and (
                elite_pattern_rank >= 0.16
                or failed_opposite_move_confirmed
                or breakout_retest_ready
                or first_pullback_ready
                or entry_confirmation_ready
            )
        )
        inactivity_execution_relief = bool(
            inactivity_relief_active
            and inactivity_flat_book
            and strong_market_candidate
            and float(signal.confidence or 0.0) >= 0.68
        )

        risk_score = 0.0
        reasons: List[str] = []
        hard_blocks: List[str] = []

        if directional_extension >= 0.90:
            risk_score += 0.48
            reasons.append("entry already stretched away from the supportive side")
        elif directional_extension >= 0.82:
            risk_score += 0.32
            reasons.append("entry is extended")

        if extension_score >= 1.25 or abs(vwap_distance_atr) >= 1.65:
            risk_score += 0.26
            reasons.append("price is too far extended from fair value")
        elif extension_score >= 1.05 or abs(vwap_distance_atr) >= 1.25:
            risk_score += 0.16
            reasons.append("price is extended from value")

        if opposing_distance <= 0.0025:
            risk_score += 0.38
            reasons.append("entry is too close to the opposing level")
        elif opposing_distance <= 0.0045:
            risk_score += 0.24
            reasons.append("entry is close to the opposing level")

        if volatility_ratio >= 1.75:
            risk_score += 0.26
            reasons.append("volatility is already expanded")
        elif volatility_ratio >= 1.45:
            risk_score += 0.16
            reasons.append("volatility is running hot")

        if exhaustion_risk >= 0.45:
            risk_score += 0.28
            reasons.append("microstructure already shows exhaustion")
        elif dominant_exhaustion >= 0.60 or bias_exhausted:
            risk_score += 0.20
            reasons.append("structure is already exhausted")

        stop_hunt_risk = float(signal.metadata.get("stop_hunt_risk", 0.0) or 0.0)
        if stop_hunt_risk >= 0.48:
            risk_score += 0.20
            reasons.append("stop-hunt risk is elevated")

        if setup_quality <= 0.35:
            risk_score += 0.12
            reasons.append("setup quality is thin")

        if candle_quality_score <= 0.22:
            risk_score += 0.24
            reasons.append("trigger candle quality is poor")
        elif candle_quality_score <= 0.34:
            risk_score += 0.14
            reasons.append("trigger candle quality is weak")

        if session_quality_score <= 0.32:
            risk_score += 0.18
            reasons.append("session quality is poor")
        elif session_quality_score <= 0.45:
            risk_score += 0.10
            reasons.append("session quality is only mediocre")

        if target_efficiency_score <= 0.18:
            risk_score += 0.22
            reasons.append("path to target is inefficient")
        elif target_efficiency_score <= 0.30:
            risk_score += 0.12
            reasons.append("path to target is tight")

        if impulse_age_bars >= 6:
            risk_score += 0.22
            reasons.append("setup is too old after the initial impulse")
        elif impulse_age_bars >= 4:
            risk_score += 0.12
            reasons.append("setup is aging after the initial impulse")

        if confirmation_needed:
            risk_score += 0.10
            reasons.append("recent pattern history suggests waiting for extra confirmation")

        if entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
            risk_score += 0.22
            reasons.append("entry confirmation delay has not completed yet")
        elif entry_confirmation_bars_required > 0 and entry_confirmation_count >= entry_confirmation_bars_required:
            risk_score -= 0.04

        if failed_opposite_move_confirmed:
            risk_score -= 0.06
        if strong_market_candidate and extension_score <= 1.18 and target_efficiency_score >= 0.22 and impulse_age_bars <= 6:
            risk_score -= 0.04
        if elite_supported_candidate and cluster_penalty < 0.22:
            risk_score -= 0.03

        policy_min_setup_quality = float(regime_entry_policy.get("min_setup_quality", 0.0) or 0.0)
        policy_min_candle_quality = float(regime_entry_policy.get("min_candle_quality", 0.0) or 0.0)
        policy_max_extension = float(regime_entry_policy.get("max_extension_score", 99.0) or 99.0)
        policy_min_target_efficiency = float(regime_entry_policy.get("min_target_efficiency", 0.0) or 0.0)
        policy_max_impulse_age = int(regime_entry_policy.get("max_impulse_age_bars", 99) or 99)

        if setup_quality < policy_min_setup_quality:
            risk_score += 0.14
            reasons.append("setup falls below regime-specific quality policy")
        if candle_quality_score < policy_min_candle_quality:
            risk_score += 0.12
            reasons.append("trigger candle falls below regime-specific quality policy")
        if extension_score > policy_max_extension:
            risk_score += 0.16
            reasons.append("entry exceeds regime-specific extension policy")
        if target_efficiency_score < policy_min_target_efficiency:
            risk_score += 0.12
            reasons.append("target path falls below regime-specific efficiency policy")
        if impulse_age_bars > policy_max_impulse_age:
            risk_score += 0.12
            reasons.append("setup is too old for the current regime policy")

        if cluster_penalty >= 0.18:
            risk_score += min(0.20, cluster_penalty)
            reasons.append("similar setups have clustered too tightly")

        if elite_pattern_rank <= 0.22 and int(recent_review.get("sample_count", 0) or 0) >= 5:
            risk_score += 0.18
            reasons.append("this setup family ranks poorly versus recent alternatives")
        elif elite_pattern_rank >= 0.72 and int(recent_review.get("sample_count", 0) or 0) >= 5:
            risk_score -= 0.06

        entry_style = str(signal.metadata.get("playbook_entry_style") or "").strip().lower()
        wants_retest = "retest" in entry_style or "pullback" in entry_style or bool(signal.metadata.get("retest_entry_preferred"))
        if wants_retest and not (breakout_retest_ready or first_pullback_ready):
            risk_score += 0.20
            reasons.append("retest quality is not confirmed yet")

        if signal.direction == "BUY" and liquidity_sweep_buy:
            risk_score -= 0.05
        elif signal.direction == "SELL" and liquidity_sweep_sell:
            risk_score -= 0.05

        if supportive_structure_distance > 0 and supportive_structure_distance <= 0.0018:
            risk_score += 0.14
            reasons.append("supportive structure is too close to anchor a durable invalidation stop")

        if late_entry_rate >= 0.28:
            risk_score += 0.08
            reasons.append("recent similar setups have started to arrive late")
        if late_entry_rate >= 0.38:
            risk_score += 0.12
            reasons.append("recent similar setups have been late")
        if late_entry_rate >= 0.45 and hard_loss_rate >= 0.30:
            risk_score += 0.18
            reasons.append("recent similar setups are losing from late timing")
        if late_entry_rate >= 0.42 and avg_rr_realized <= -0.20:
            risk_score += 0.10
            reasons.append("similar setups are failing to earn enough reward after entry")
        if avg_quality_score <= 46.0 and hard_loss_rate >= 0.28:
            risk_score += 0.08
            reasons.append("recent execution quality for this setup family is poor")
        if inactivity_execution_relief:
            risk_score -= 0.04 + inactivity_relief_strength * 0.08

        if broker_agreement_state in {"divergent", "severe_divergence"} and broker_spread_regime in {"stressed", "extreme", "wide"}:
            hard_blocks.append("broker divergence and spread stress are both active")
        weak_candle_extension_limit = 1.32 if elite_supported_candidate else 1.25
        weak_candle_floor = 0.24 if elite_supported_candidate else 0.26
        if inactivity_execution_relief:
            weak_candle_extension_limit += 0.02 + inactivity_relief_strength * 0.03
            weak_candle_floor = max(0.22, weak_candle_floor - (0.01 + inactivity_relief_strength * 0.02))
        if extension_score >= weak_candle_extension_limit and candle_quality_score <= weak_candle_floor:
            hard_blocks.append("entry is extended and the trigger candle is weak")
        target_efficiency_hard_floor = 0.11 if elite_supported_candidate else 0.15
        opposing_distance_hard_floor = 0.0030 if elite_supported_candidate else 0.0035
        if inactivity_execution_relief:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - (0.015 + inactivity_relief_strength * 0.03))
            opposing_distance_hard_floor = max(0.0024, opposing_distance_hard_floor - (0.0003 + inactivity_relief_strength * 0.0004))
        if target_efficiency_score <= target_efficiency_hard_floor and opposing_distance <= opposing_distance_hard_floor:
            hard_blocks.append("too little clean space remains to the target")
        impulse_age_hard_limit = 7 if elite_supported_candidate else 6
        directional_extension_hard_limit = 0.80 if elite_supported_candidate else 0.74
        if inactivity_execution_relief:
            impulse_age_hard_limit += 1 + (1 if inactivity_relief_strength >= 0.75 else 0)
            directional_extension_hard_limit += 0.03 + inactivity_relief_strength * 0.05
        if impulse_age_bars >= impulse_age_hard_limit and directional_extension >= directional_extension_hard_limit:
            hard_blocks.append("setup is too old and already stretched")
        if wants_retest and not breakout_retest_ready and not first_pullback_ready:
            hard_blocks.append("breakout entry has not earned a clean retest or first pullback")
        if directional_extension >= 0.82 and (
            exhaustion_risk >= 0.45 or dominant_exhaustion >= 0.60 or bias_exhausted
        ):
            hard_blocks.append("extended entry is combining with exhaustion risk")
        if stop_hunt_risk >= 0.48 and synthetic_depth_only:
            hard_blocks.append("stop-hunt risk is elevated while only synthetic depth is available")
        if directional_extension >= 0.82 and supportive_structure_distance > 0 and supportive_structure_distance <= 0.0018:
            hard_blocks.append("late entry profile has poor structure distance")
        if cross_asset_confidence >= 0.20 and cross_asset_alignment <= -0.20 and setup_quality <= 0.35:
            hard_blocks.append("cross-asset conflict is present while setup quality is weak")
        if broker_quote_quality_state in {"stale", "delayed"} and stop_hunt_risk >= 0.48:
            hard_blocks.append("quote quality is stale while stop-hunt risk is elevated")
        if blocked_recent_pattern and blocked_recent_pattern_reason:
            hard_blocks.append(blocked_recent_pattern_reason)
        if late_entry_rate >= 0.45 and hard_loss_rate >= 0.30 and directional_extension >= 0.74:
            hard_blocks.append("recent pattern learning shows this entry shape keeps arriving too late")
        if entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
            hard_blocks.append("entry confirmation delay is still pending")
        pattern_rank_hard_floor = 0.08 if strong_market_candidate and setup_quality >= 0.66 and alignment_score >= 0.74 else 0.12
        if inactivity_execution_relief:
            pattern_rank_hard_floor = max(0.04, pattern_rank_hard_floor - (0.015 + inactivity_relief_strength * 0.035))
        if pattern_family != "unknown" and elite_pattern_rank <= pattern_rank_hard_floor:
            hard_blocks.append("pattern family ranks below elite threshold")
        cluster_hard_limit = 0.30 if elite_supported_candidate else 0.26
        if cluster_penalty >= cluster_hard_limit:
            hard_blocks.append("trade clustering risk is too high")
        if regime_entry_policy:
            if setup_quality < float(regime_entry_policy.get("min_setup_quality", 0.0) or 0.0) and candle_quality_score <= float(
                regime_entry_policy.get("min_candle_quality", 0.0) or 0.0
            ):
                hard_blocks.append("regime-specific entry policy rejects the setup")

        signal.metadata["late_entry_risk_score"] = round(risk_score, 4)
        signal.metadata["late_entry_risk_reasons"] = list(reasons)
        signal.metadata["execution_hard_blocks"] = list(hard_blocks)
        signal.metadata["execution_relief_flags"] = {
            "strong_market_candidate": strong_market_candidate,
            "strong_fx_crypto_candidate": strong_fx_crypto_candidate,
            "elite_supported_candidate": elite_supported_candidate,
            "inactivity_execution_relief": inactivity_execution_relief,
            "inactivity_relief_strength": round(inactivity_relief_strength, 4),
            "inactivity_flat_book": inactivity_flat_book,
        }
        data["late_entry_risk"] = {
            "score": round(risk_score, 4),
            "directional_extension": round(directional_extension, 4),
            "opposing_distance": round(opposing_distance, 6),
            "supportive_structure_distance": round(supportive_structure_distance, 6),
            "volatility_ratio": round(volatility_ratio, 4),
            "exhaustion_risk": round(exhaustion_risk, 4),
            "dominant_exhaustion": round(dominant_exhaustion, 4),
            "stop_hunt_risk": round(stop_hunt_risk, 4),
            "late_entry_rate": round(late_entry_rate, 4),
            "hard_loss_rate": round(hard_loss_rate, 4),
            "avg_rr_realized": round(avg_rr_realized, 4),
            "avg_quality_score": round(avg_quality_score, 2),
            "inactivity_hours_since_last_entry": round(inactivity_hours_since_last_entry, 2),
            "inactivity_relief_strength": round(inactivity_relief_strength, 4),
            "blocked_recent_pattern": blocked_recent_pattern,
            "blocked_recent_pattern_reason": blocked_recent_pattern_reason,
            "confirmation_needed": confirmation_needed,
            "pattern_rank_score": round(elite_pattern_rank, 4),
            "trade_cluster_penalty": round(cluster_penalty, 4),
            "pattern_family": pattern_family,
            "failed_opposite_move_confirmed": failed_opposite_move_confirmed,
            "entry_confirmation_bars_required": int(entry_confirmation_bars_required),
            "entry_confirmation_count": int(entry_confirmation_count),
            "entry_confirmation_ready": entry_confirmation_ready,
            "regime_entry_policy": regime_entry_policy,
            "vwap_distance_atr": round(vwap_distance_atr, 4),
            "session_quality_score": round(session_quality_score, 4),
            "candle_quality_score": round(candle_quality_score, 4),
            "extension_score": round(extension_score, 4),
            "target_efficiency_score": round(target_efficiency_score, 4),
            "impulse_age_bars": int(impulse_age_bars),
            "breakout_retest_ready": breakout_retest_ready,
            "first_pullback_ready": first_pullback_ready,
            "liquidity_sweep_buy": liquidity_sweep_buy,
            "liquidity_sweep_sell": liquidity_sweep_sell,
            "alignment_score": round(alignment_score, 4),
            "strong_market_candidate": strong_market_candidate,
            "strong_fx_crypto_candidate": strong_fx_crypto_candidate,
            "elite_supported_candidate": elite_supported_candidate,
            "hard_blocks": list(hard_blocks),
            "reasons": list(reasons),
        }
        if hard_blocks:
            direction_label = "buy" if signal.direction == "BUY" else "sell"
            return self._kill_review(
                signal,
                step=STEP_EXECUTION,
                name="execution",
                reason=f"execution hard block on {direction_label}: {'; '.join(hard_blocks[:2])}",
                conf_before=conf_before,
                data=data,
            )

        if risk_score >= 0.58:
            direction_label = "buy" if signal.direction == "BUY" else "sell"
            summary = "; ".join(reasons[:3]) if reasons else "entry is already too late"
            return self._kill_review(
                signal,
                step=STEP_EXECUTION,
                name="execution",
                reason=f"late entry risk on {direction_label}: {summary}",
                conf_before=conf_before,
                data=data,
            )

        if risk_score >= 0.52:
            notes.append("late_entry_risk")
        return True

    @staticmethod
    def _execution_align_structure_target(
        signal: Signal,
        risk_manager: Any,
        category: str,
        structure: Dict[str, Any],
        has_managed_target_plan: bool,
        data: Dict[str, Any],
    ) -> None:
        align_tp_fn = getattr(risk_manager, "align_take_profit_to_structure", None)
        if not (
            callable(align_tp_fn)
            and signal.entry_price
            and signal.stop_loss
            and signal.take_profit
            and not has_managed_target_plan
        ):
            return
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
            if not isinstance(aligned_tp, (int, float)) or aligned_tp <= 0:
                return
            aligned_tp = float(aligned_tp)
            previous_tp = float(signal.take_profit)
            if abs(aligned_tp - previous_tp) <= 1e-9:
                return
            risk = abs(float(signal.entry_price) - float(signal.stop_loss))
            adjusted_rr = (
                abs(aligned_tp - float(signal.entry_price)) / risk
                if risk > 0
                else float(signal.risk_reward or 0.0)
            )
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

    @staticmethod
    def _execution_apply_rr_floor(
        signal: Signal,
        adaptive_min_rr: float,
        data: Dict[str, Any],
        notes: List[str],
    ) -> bool:
        if adaptive_min_rr <= 0 or float(signal.risk_reward or 0.0) >= adaptive_min_rr:
            return True
        rr_gap = max(0.0, adaptive_min_rr - float(signal.risk_reward or 0.0))
        signal.metadata["adaptive_rr_gap"] = round(rr_gap, 4)
        data["adaptive_rr_gap"] = round(rr_gap, 4)
        data["min_required_rr"] = round(adaptive_min_rr, 4)
        data["actual_rr"] = round(float(signal.risk_reward or 0.0), 4)
        notes.append("rr_below_policy")
        return rr_gap <= 0.12

    @staticmethod
    def _execution_apply_scorecard(signal: Signal, context: Dict[str, Any], data: Dict[str, Any]) -> None:
        try:
            from services.signal_scorecard import get_service as get_signal_scorecard_service

            scorecard = get_signal_scorecard_service().score(signal, context)
            signal.confidence = float(scorecard.get("final_score", signal.confidence) or signal.confidence)
            signal.metadata["scorecard"] = scorecard
            signal.metadata["live_validation_profile"] = dict(scorecard.get("live_validation") or {})
            signal.metadata["execution_expectancy_profile"] = dict(scorecard.get("execution_expectancy") or {})
            data["scorecard"] = {
                "raw_score": scorecard.get("raw_score"),
                "reliability": scorecard.get("reliability"),
                "breakdown": dict(scorecard.get("breakdown") or {}),
                "execution_expectancy": dict(scorecard.get("execution_expectancy") or {}),
                "notes": list(scorecard.get("notes") or []),
            }
        except Exception as exc:
            logger.debug(f"[DecisionEngine] Signal scorecard unavailable for {signal.asset}: {exc}")

    @staticmethod
    def _execution_confidence_gate(
        signal: Signal,
        conf_before: float,
        min_final_conf: float,
        data: Dict[str, Any],
    ) -> bool:
        if signal.confidence > min_final_conf:
            return True
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

    @staticmethod
    def _execution_apply_position_sizing(
        signal: Signal,
        risk_manager: Any,
        adaptive_risk_multiplier: float,
        data: Dict[str, Any],
    ) -> None:
        if not risk_manager:
            return
        try:
            sizing_confidence = min(
                MAX_SIGNAL_CONFIDENCE,
                max(MIN_CONFIDENCE_SCORE, signal.confidence * adaptive_risk_multiplier),
            )
            size = risk_manager.calculate_position_size(
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

    @staticmethod
    def _execution_ensure_take_profit_levels(signal: Signal) -> None:
        if not (signal.entry_price and signal.take_profit) or signal.take_profit_levels:
            return
        try:
            entry = signal.entry_price
            tp1 = signal.take_profit
            dist = abs(tp1 - entry)
            if dist <= 0:
                return
            if signal.direction == "BUY":
                signal.take_profit_levels = [
                    round(entry + dist * 0.5, 6),
                    round(entry + dist, 6),
                    round(entry + dist * 1.5, 6),
                ]
            else:
                signal.take_profit_levels = [
                    round(entry - dist * 0.5, 6),
                    round(entry - dist, 6),
                    round(entry - dist * 1.5, 6),
                ]
        except Exception as exc:
            logger.debug(f"[DecisionEngine] TP level calculation failed for {signal.asset}: {exc}")

    def _apply_execution_review(self, signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        price = signal.entry_price
        spread = context.get("spread")
        category = context.get("category", signal.category or "forex")
        data: Dict[str, Any] = {}
        notes: List[str] = []
        engine = context.get("engine")
        risk_manager = getattr(engine, "_risk_manager", None) if engine else None
        management_plan = (
            signal.metadata.get("trade_management_plan")
            if isinstance(signal.metadata.get("trade_management_plan"), dict)
            else {}
        )
        staged_targets = self._execution_staged_targets(signal)
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

        structure = context.get("market_structure") or signal.metadata.get("market_structure") or {}
        self._execution_entry_quality(signal, context.get("price_data"), structure if isinstance(structure, dict) else {}, data, notes)

        structure = structure if isinstance(structure, dict) else {}
        if not self._execution_late_entry_risk_gate(
            signal,
            adaptive_policy=adaptive_policy,
            conf_before=conf_before,
            structure=structure,
            data=data,
            notes=notes,
        ):
            return False
        self._execution_align_structure_target(
            signal,
            risk_manager,
            category,
            structure,
            has_managed_target_plan,
            data,
        )

        if has_managed_target_plan:
            self._execution_sync_managed_targets(signal, staged_targets, data)

        if not self._execution_spread_gate(signal, spread, price, max_spread_pct, conf_before, data, notes):
            return False

        if not self._execution_apply_rr_floor(signal, adaptive_min_rr, data, notes):
            return self._kill_review(
                signal,
                step=STEP_EXECUTION,
                name="execution",
                reason=(
                    f"rr below adaptive floor: "
                    f"{float(signal.risk_reward or 0.0):.2f} < {float(adaptive_min_rr):.2f}"
                ),
                conf_before=conf_before,
                data=data,
            )

        signal.metadata["execution_review_notes"] = list(notes)
        data["notes"] = list(notes)

        self._execution_apply_scorecard(signal, context, data)

        if not self._execution_confidence_gate(signal, conf_before, min_final_conf, data):
            return False

        self._execution_apply_position_sizing(signal, risk_manager, adaptive_risk_multiplier, data)
        self._execution_ensure_take_profit_levels(signal)

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

    @staticmethod
    def _apply_memory_review(signal: Signal, context: Dict[str, Any]) -> bool:
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
        sample_count = int(memory.get("sample_count", 0) or 0)
        memory_edge = float(memory.get("memory_edge", 0.0) or 0.0)
        memory_score = float(memory.get("memory_score", 50.0) or 50.0)
        memory_notes = list(memory.get("notes", []) or [])

        if adjustment > 0:
            signal.boost(adjustment)
        elif adjustment < 0:
            signal.reduce(abs(adjustment))

        signal.metadata["memory_adjustment_applied"] = round(adjustment, 4)
        signal.metadata["memory_notes"] = list(memory_notes)

        strong_negative_memory = bool(
            sample_count >= 8 and (memory_edge <= -0.18 or memory_score <= 36.0)
        )
        if strong_negative_memory:
            reason = (
                f"negative setup memory: score={memory_score:.1f} "
                f"edge={memory_edge:+.3f} samples={sample_count}"
            )
            signal.kill(reason, 0)
            signal.journal.record(
                layer=0,
                name="memory",
                decision=KILLED,
                reason=reason,
                conf_before=conf_before,
                conf_after=signal.confidence,
                data={
                    "memory_score": memory_score,
                    "memory_edge": memory_edge,
                    "memory_win_rate": memory.get("win_rate"),
                    "memory_similarity": memory.get("avg_similarity"),
                    "memory_sample_count": sample_count,
                    "same_asset_matches": memory.get("same_asset_matches"),
                    "adjustment": adjustment,
                    "notes": memory_notes,
                    "fingerprint": fingerprint,
                },
            )
            return False

        reason = (
            f"memory score={memory_score:.1f} "
            f"edge={memory_edge:+.3f} "
            f"samples={sample_count} "
            f"adj={adjustment:+.3f}"
        )
        signal.journal.record(
            layer=0,
            name="memory",
            decision=INFO,
            reason=reason,
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "memory_score": memory_score,
                "memory_edge": memory_edge,
                "memory_win_rate": memory.get("win_rate"),
                "memory_similarity": memory.get("avg_similarity"),
                "memory_sample_count": sample_count,
                "same_asset_matches": memory.get("same_asset_matches"),
                "adjustment": adjustment,
                "notes": memory_notes,
                "fingerprint": fingerprint,
            },
        )
        return True

    @staticmethod
    def _apply_policy_review(signal: Signal, context: Dict[str, Any]) -> bool:
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

    @staticmethod
    def _apply_governance_review(signal: Signal, context: Dict[str, Any]) -> bool:
        conf_before = signal.confidence
        profile = get_profile(signal.asset)
        valid_sources = count_valid_sources(signal)
        min_required = profile.min_valid_layers
        valid_families = list(signal.metadata.get("valid_source_families") or [])
        stale_families = list(signal.metadata.get("stale_source_families") or [])
        signal.metadata["valid_sources_count"] = valid_sources
        signal.metadata["min_sources_required"] = min_required
        data = {
            "valid_sources": valid_sources,
            "min_required": min_required,
            "category": signal.category,
            "valid_source_families": valid_families,
            "stale_source_families": stale_families,
            "source_family_evidence": dict(signal.metadata.get("source_family_evidence") or {}),
        }

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

    @staticmethod
    def _finalize(
        signal: Signal,
        context: Dict[str, Any],
        *,
        report: bool = True,
        keep_dead: bool = False,
    ) -> Optional[Signal]:
        elapsed_ms = (time.monotonic() - context.get("decision_start", time.monotonic())) * 1000

        if os.getenv("DEBUG_FORCE_SURVIVE", "0") == "1" and not signal.alive:
            signal.alive = True
            signal.kill_reason = "Forced survive via DEBUG_FORCE_SURVIVE"

        if report:
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
                            "predictor_prediction": context.get("predictor_prediction", context.get("ml_prediction")),
                            "predictor_confidence": context.get("predictor_confidence", context.get("ml_confidence")),
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

        return signal if (signal.alive or keep_dead) else None

    def preview(self, signal: Signal, context: Optional[Dict[str, Any]] = None) -> Signal:
        context = context or {}
        context.setdefault("decision_start", time.monotonic())
        try:
            if not self._apply_market_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
            if not self._apply_intelligence_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
            if not self._apply_memory_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
            if not self._apply_policy_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
            if not self._apply_governance_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
            if not self._apply_execution_review(signal, context):
                return self._finalize(signal, context, report=False, keep_dead=True) or signal
        except Exception as exc:
            if signal.alive:
                signal.kill(f"decision engine preview exception: {exc}", STEP_GOVERNANCE)
            signal.journal.record(
                layer=STEP_GOVERNANCE,
                name="governance",
                decision=KILLED,
                reason=f"preview exception: {exc}",
                conf_before=signal.confidence,
                conf_after=signal.confidence,
            )
        return self._finalize(signal, context, report=False, keep_dead=True) or signal


decision_engine = SignalDecisionEngine()
