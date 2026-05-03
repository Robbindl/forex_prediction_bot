from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from config.config import (
    MAX_SIGNAL_CONFIDENCE,
    MIN_CONFIDENCE_SCORE,
    MIN_FINAL_CONFIDENCE,
    PLAYBOOK_ONLY_RUNTIME,
    SPREAD_THRESHOLDS,
    get_trading_timeframe,
)
from core.asset_profiles import get_execution_policy, get_profile
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

_PREFERRED_TRUE_DEPTH_SOURCES = {
    "order_flow_true_depth",
    "dukascopy_live_depth",
    "ctrader_live_depth",
    "binance_rest_depth",
    "binance_live_depth",
}
_EXCHANGE_TRUE_DEPTH_PROVIDERS = {"binance", "bybit", "okx"}
_SIDECAR_TRUE_DEPTH_PROVIDERS = {"dukascopy", "ctrader"}


def _true_depth_provider_kind(
    microstructure_source: str,
    depth_provider: str,
    depth_provider_class: str,
) -> str:
    source = str(microstructure_source or "").strip().lower()
    provider = str(depth_provider or "").strip().lower()
    provider_class = str(depth_provider_class or "").strip().lower()
    if (
        provider_class == "exchange_depth"
        or any(token in provider for token in _EXCHANGE_TRUE_DEPTH_PROVIDERS)
        or source in {"binance_rest_depth", "binance_live_depth"}
        or (
            source == "live_store_depth"
            and any(token in provider for token in _EXCHANGE_TRUE_DEPTH_PROVIDERS)
        )
    ):
        return "exchange"
    if provider_class == "redis_subscriber" or source == "order_flow_true_depth":
        return "redis"
    if (
        provider_class == "sidecar"
        or source in {"dukascopy_live_depth", "ctrader_live_depth"}
        or any(token in provider for token in _SIDECAR_TRUE_DEPTH_PROVIDERS)
    ):
        return "sidecar"
    return ""


def _is_preferred_true_depth_source(
    microstructure_source: str,
    depth_provider: str,
    depth_provider_class: str,
) -> bool:
    source = str(microstructure_source or "").strip().lower()
    provider = str(depth_provider or "").strip().lower()
    provider_class = str(depth_provider_class or "").strip().lower()
    if source in _PREFERRED_TRUE_DEPTH_SOURCES:
        return True
    if source == "live_store_depth":
        return bool(
            provider_class in {"exchange_depth", "redis_subscriber", "sidecar"}
            or any(token in provider for token in (*_EXCHANGE_TRUE_DEPTH_PROVIDERS, *_SIDECAR_TRUE_DEPTH_PROVIDERS))
        )
    if provider_class in {"exchange_depth", "redis_subscriber"}:
        return True
    return any(token in provider for token in (*_EXCHANGE_TRUE_DEPTH_PROVIDERS, *_SIDECAR_TRUE_DEPTH_PROVIDERS, "orderflow"))


def _trusted_snapshot_true_depth_source(
    *,
    microstructure_source: str,
    depth_provider: str,
    depth_provider_class: str,
    depth_levels: int,
    snapshot_true_depth_min_levels: int,
) -> bool:
    source = str(microstructure_source or "").strip().lower()
    provider = str(depth_provider or "").strip().lower()
    provider_class = str(depth_provider_class or "").strip().lower()
    if source in {"binance_rest_depth", "binance_live_depth"}:
        return depth_levels >= snapshot_true_depth_min_levels
    if provider_class == "exchange_depth" or any(token in provider for token in _EXCHANGE_TRUE_DEPTH_PROVIDERS):
        return depth_levels >= snapshot_true_depth_min_levels
    if source in {"dukascopy_live_depth", "ctrader_live_depth"} or any(
        token in provider for token in _SIDECAR_TRUE_DEPTH_PROVIDERS
    ):
        return depth_levels >= 2
    return False


_REAL_DOM_DEPTH_UPDATE_MODES = {
    "event_stream",
    "ladder_stream",
    "delta_stream",
    "depth_stream",
    "snapshot_poll",
    "stream_snapshot",
    "snapshot_stream",
}
_UNTRUSTED_DOM_DEPTH_UPDATE_MODES = {"", "none", "synthetic", "top_quote", "top_of_book"}


def _metadata_age_seconds(metadata: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = metadata.get(key)
        if value in (None, ""):
            continue
        try:
            age = float(value)
        except Exception:
            continue
        if age >= 0.0:
            return age
    return None


def _real_dom_book_is_fresh(
    *,
    metadata: Dict[str, Any],
    depth_update_mode: str,
    depth_provider: str,
    depth_provider_class: str,
    microstructure_source: str,
) -> bool:
    update_mode = str(depth_update_mode or "").strip().lower()
    if update_mode in _UNTRUSTED_DOM_DEPTH_UPDATE_MODES:
        return False

    provider = str(depth_provider or "").strip().lower()
    provider_class = str(depth_provider_class or "").strip().lower()
    source = str(microstructure_source or "").strip().lower()
    is_sidecar = bool(
        source in {"dukascopy_live_depth", "ctrader_live_depth"}
        or provider_class == "sidecar"
        or any(token in provider for token in _SIDECAR_TRUE_DEPTH_PROVIDERS)
    )
    max_age = 90.0 if is_sidecar else 30.0
    age = _metadata_age_seconds(
        metadata,
        "dom_depth_event_age_seconds",
        "depth_live_age_seconds",
        "dom_depth_stream_age_seconds",
        "dom_stream_last_message_age_seconds",
    )
    if age is not None:
        return age <= max_age

    if bool(metadata.get("dom_stream_snapshot_ready")):
        return True
    if update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}:
        return True
    if update_mode in _REAL_DOM_DEPTH_UPDATE_MODES:
        return (
            int(metadata.get("dom_snapshot_count", 0) or 0) > 0
            or int(metadata.get("dom_delta_count", 0) or 0) > 0
        )
    return False


def _trusted_real_dom_book_available(
    *,
    metadata: Dict[str, Any],
    true_depth_available: bool,
    preferred_true_depth: bool,
    microstructure_source: str,
    depth_provider: str,
    depth_provider_class: str,
    depth_update_mode: str,
    depth_levels: int,
    snapshot_true_depth_min_levels: int,
    depth_quality: float,
    min_quality: float,
    raw_depth_trust_score: float,
    min_trust_score: float,
    depth_quote_alignment_score: float,
    true_depth_quote_aligned: bool,
    depth_fragmentation_untrusted: bool = False,
) -> bool:
    if not true_depth_available or not preferred_true_depth:
        return False
    if not _trusted_snapshot_true_depth_source(
        microstructure_source=microstructure_source,
        depth_provider=depth_provider,
        depth_provider_class=depth_provider_class,
        depth_levels=depth_levels,
        snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
    ):
        return False
    return bool(
        str(depth_update_mode or "").strip().lower() not in _UNTRUSTED_DOM_DEPTH_UPDATE_MODES
        and depth_quality >= min_quality
        and raw_depth_trust_score >= min_trust_score
        and depth_quote_alignment_score >= 0.80
        and true_depth_quote_aligned
        and not depth_fragmentation_untrusted
        and _real_dom_book_is_fresh(
            metadata=metadata,
            depth_update_mode=depth_update_mode,
            depth_provider=depth_provider,
            depth_provider_class=depth_provider_class,
            microstructure_source=microstructure_source,
        )
    )

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


def _effective_dom_stream_trust_metrics(
    signal: Signal,
    execution_policy: Dict[str, Any],
    *,
    base_trust_score: float,
) -> Dict[str, Any]:
    metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
    dom_authority_tier = str(metadata.get("dom_authority_tier") or "").strip().lower()
    dom_ladder_ready = bool(metadata.get("dom_ladder_ready"))
    health_known = bool(metadata.get("dom_stream_health_known"))
    health_score = max(0.0, min(1.0, _safe_float(metadata.get("dom_stream_health_score"), 1.0)))
    trust_decay = max(0.0, min(1.0, _safe_float(metadata.get("dom_stream_trust_decay"), 0.0)))
    stream_degraded = bool(metadata.get("dom_stream_degraded"))
    depth_stream_missing = bool(metadata.get("dom_depth_stream_missing"))
    trade_stream_missing = bool(metadata.get("dom_trade_stream_missing"))
    event_authority = dom_ladder_ready or dom_authority_tier in {
        "event_ladder",
        "fragmented_event_ladder",
        "degraded_event_ladder",
    }
    min_sovereignty_score = _safe_float(
        execution_policy.get("dom_stream_health_min_score_for_sovereignty"),
        0.58,
    )
    hard_floor = _safe_float(execution_policy.get("dom_stream_health_hard_floor"), 0.34)
    penalty_scale = _safe_float(execution_policy.get("dom_stream_trust_penalty_scale"), 0.60)
    degraded_penalty = _safe_float(execution_policy.get("dom_stream_degraded_penalty"), 0.12)

    effective_trust_score = float(base_trust_score or 0.0)
    if health_known and event_authority:
        effective_trust_score *= max(0.20, 1.0 - trust_decay * penalty_scale)
        if health_score < min_sovereignty_score:
            effective_trust_score *= max(0.45, health_score / max(min_sovereignty_score, 1e-6))
        if stream_degraded or depth_stream_missing:
            effective_trust_score = max(0.0, effective_trust_score - degraded_penalty)
        if trade_stream_missing and dom_authority_tier == "event_ladder":
            effective_trust_score = max(0.0, effective_trust_score - degraded_penalty * 0.5)

    sovereignty_supported = bool(
        not health_known
        or not event_authority
        or (
            health_score >= min_sovereignty_score
            and not stream_degraded
            and not depth_stream_missing
        )
    )
    hard_floor_breached = bool(
        health_known
        and event_authority
        and (
            health_score < hard_floor
            or depth_stream_missing
        )
    )
    return {
        "health_known": health_known,
        "health_score": round(health_score, 4),
        "trust_decay": round(trust_decay, 4),
        "stream_degraded": stream_degraded,
        "depth_stream_missing": depth_stream_missing,
        "trade_stream_missing": trade_stream_missing,
        "effective_trust_score": round(max(0.0, effective_trust_score), 4),
        "sovereignty_supported": sovereignty_supported,
        "hard_floor_breached": hard_floor_breached,
    }


def _normalize_trade_direction_label(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {
        "BUY",
        "BULL",
        "BULLISH",
        "LONG",
        "ACCUMULATION",
        "INFLOW",
        "HIGH_LONG",
        "EXTREME_LONG",
        "RISK_ON",
        "UP",
    }:
        return "BUY"
    if raw in {
        "SELL",
        "BEAR",
        "BEARISH",
        "SHORT",
        "DISTRIBUTION",
        "OUTFLOW",
        "HIGH_SHORT",
        "EXTREME_SHORT",
        "RISK_OFF",
        "DOWN",
    }:
        return "SELL"
    return ""


_SESSION_TIMING_STRICTNESS_BY_CATEGORY: Dict[str, Dict[str, Dict[str, Any]]] = {
    "indices": {
        "thin_session": {
            "risk_penalty": 0.04,
            "weak_candle_extension_delta": -0.04,
            "weak_candle_floor_delta": 0.01,
            "target_efficiency_floor_delta": 0.03,
            "opposing_distance_floor_delta": 0.00025,
            "impulse_age_limit_delta": -1,
            "directional_extension_limit_delta": -0.03,
            "pattern_rank_floor_delta": 0.02,
            "require_confirmation": False,
        },
        "off_session": {
            "risk_penalty": 0.08,
            "weak_candle_extension_delta": -0.08,
            "weak_candle_floor_delta": 0.02,
            "target_efficiency_floor_delta": 0.06,
            "opposing_distance_floor_delta": 0.00055,
            "impulse_age_limit_delta": -2,
            "directional_extension_limit_delta": -0.08,
            "pattern_rank_floor_delta": 0.05,
            "require_confirmation": True,
        },
    },
    "commodities": {
        "thin_session": {
            "risk_penalty": 0.035,
            "weak_candle_extension_delta": -0.035,
            "weak_candle_floor_delta": 0.01,
            "target_efficiency_floor_delta": 0.025,
            "opposing_distance_floor_delta": 0.00020,
            "impulse_age_limit_delta": -1,
            "directional_extension_limit_delta": -0.025,
            "pattern_rank_floor_delta": 0.02,
            "require_confirmation": False,
        },
        "off_session": {
            "risk_penalty": 0.07,
            "weak_candle_extension_delta": -0.07,
            "weak_candle_floor_delta": 0.02,
            "target_efficiency_floor_delta": 0.05,
            "opposing_distance_floor_delta": 0.00045,
            "impulse_age_limit_delta": -2,
            "directional_extension_limit_delta": -0.07,
            "pattern_rank_floor_delta": 0.04,
            "require_confirmation": True,
        },
    },
    "forex": {
        "thin_session": {
            "risk_penalty": 0.02,
            "weak_candle_extension_delta": -0.02,
            "weak_candle_floor_delta": 0.005,
            "target_efficiency_floor_delta": 0.015,
            "opposing_distance_floor_delta": 0.00012,
            "impulse_age_limit_delta": 0,
            "directional_extension_limit_delta": -0.02,
            "pattern_rank_floor_delta": 0.01,
            "require_confirmation": False,
        },
        "off_session": {
            "risk_penalty": 0.05,
            "weak_candle_extension_delta": -0.05,
            "weak_candle_floor_delta": 0.01,
            "target_efficiency_floor_delta": 0.03,
            "opposing_distance_floor_delta": 0.00030,
            "impulse_age_limit_delta": -1,
            "directional_extension_limit_delta": -0.05,
            "pattern_rank_floor_delta": 0.03,
            "require_confirmation": False,
        },
    },
    "crypto": {
        "thin_session": {
            "risk_penalty": 0.01,
            "weak_candle_extension_delta": -0.01,
            "weak_candle_floor_delta": 0.0,
            "target_efficiency_floor_delta": 0.01,
            "opposing_distance_floor_delta": 0.00010,
            "impulse_age_limit_delta": 0,
            "directional_extension_limit_delta": -0.01,
            "pattern_rank_floor_delta": 0.0,
            "require_confirmation": False,
        },
        "off_session": {
            "risk_penalty": 0.02,
            "weak_candle_extension_delta": -0.02,
            "weak_candle_floor_delta": 0.005,
            "target_efficiency_floor_delta": 0.015,
            "opposing_distance_floor_delta": 0.00015,
            "impulse_age_limit_delta": -1,
            "directional_extension_limit_delta": -0.02,
            "pattern_rank_floor_delta": 0.01,
            "require_confirmation": False,
        },
    },
}


def _session_timing_strictness(
    category: str,
    session_quality_label: str,
    session_quality_score: float,
) -> Dict[str, Any]:
    label = str(session_quality_label or "").strip().lower()
    if label not in {"thin_session", "off_session"}:
        if session_quality_score <= 0.28:
            label = "off_session"
        elif session_quality_score <= 0.42:
            label = "thin_session"
        else:
            return {
                "label": "normal",
                "risk_penalty": 0.0,
                "weak_candle_extension_delta": 0.0,
                "weak_candle_floor_delta": 0.0,
                "target_efficiency_floor_delta": 0.0,
                "opposing_distance_floor_delta": 0.0,
                "impulse_age_limit_delta": 0,
                "directional_extension_limit_delta": 0.0,
                "pattern_rank_floor_delta": 0.0,
                "require_confirmation": False,
                "reason": "",
            }
    category_key = str(category or "").strip().lower()
    category_rules = _SESSION_TIMING_STRICTNESS_BY_CATEGORY.get(
        category_key,
        _SESSION_TIMING_STRICTNESS_BY_CATEGORY["forex"],
    )
    strictness = dict(category_rules.get(label, {}))
    strictness.setdefault("risk_penalty", 0.0)
    strictness.setdefault("weak_candle_extension_delta", 0.0)
    strictness.setdefault("weak_candle_floor_delta", 0.0)
    strictness.setdefault("target_efficiency_floor_delta", 0.0)
    strictness.setdefault("opposing_distance_floor_delta", 0.0)
    strictness.setdefault("impulse_age_limit_delta", 0)
    strictness.setdefault("directional_extension_limit_delta", 0.0)
    strictness.setdefault("pattern_rank_floor_delta", 0.0)
    strictness.setdefault("require_confirmation", False)
    strictness["label"] = label
    strictness["reason"] = (
        "session fit is off-peak for this asset"
        if label == "off_session"
        else "session fit is thin for this asset"
    )
    return strictness


def _should_kill_for_negative_memory(
    *,
    sample_count: int,
    same_asset_matches: int,
    avg_similarity: float,
    memory_edge: float,
    memory_score: float,
) -> bool:
    if sample_count < 10:
        return False
    if avg_similarity < 0.72 and not (sample_count >= 14 and avg_similarity >= 0.66):
        return False
    if same_asset_matches < 4 and sample_count < 14:
        return False
    if memory_edge <= -0.22 or memory_score <= 34.0:
        return True
    return bool(
        same_asset_matches >= 6
        and avg_similarity >= 0.78
        and memory_edge <= -0.18
        and memory_score <= 38.0
    )


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
    if "coingecko_global" in market_intelligence_details:
        positioning_entries.append("detail:coingecko_global")
    if "cftc_positioning" in market_intelligence_components:
        positioning_entries.append("component:cftc_positioning")
    if "btc_dominance" in market_intelligence_components:
        positioning_entries.append("component:btc_dominance")
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
            trade_flow_score = float(micro.get("trade_flow_score", 0.0) or 0.0)
            trade_delta_ratio = float(micro.get("trade_delta_ratio", 0.0) or 0.0)
            trade_cvd_slope = float(micro.get("trade_cvd_slope", 0.0) or 0.0)
            velocity_bps = float(micro.get("velocity_bps", 0.0) or 0.0)
            dom_liquidity_shift_proxy = float(micro.get("dom_liquidity_shift_proxy", 0.0) or 0.0)
            dom_sweep_pressure_proxy = float(micro.get("dom_sweep_pressure_proxy", 0.0) or 0.0)
            dom_refill_resilience_proxy = float(micro.get("dom_refill_resilience_proxy", 0.0) or 0.0)
            dom_absorption_proxy = float(micro.get("dom_absorption_proxy", 0.0) or 0.0)
            dom_iceberg_proxy = float(micro.get("dom_iceberg_proxy", 0.0) or 0.0)
            dom_queue_persistence = float(micro.get("dom_queue_persistence", 0.0) or 0.0)
            dom_add_intent_bias = float(micro.get("dom_add_intent_bias", 0.0) or 0.0)
            dom_cancel_pressure_bias = float(micro.get("dom_cancel_pressure_bias", 0.0) or 0.0)
            dom_queue_erosion_bias = float(micro.get("dom_queue_erosion_bias", 0.0) or 0.0)
            dom_trade_absorption_proxy = float(micro.get("dom_trade_absorption_proxy", 0.0) or 0.0)
            dom_refill_after_sweep_bias = float(micro.get("dom_refill_after_sweep_bias", 0.0) or 0.0)
            dom_trade_aggression_bias = float(micro.get("dom_trade_aggression_bias", 0.0) or 0.0)
            dom_trade_backed_iceberg_proxy = float(micro.get("dom_trade_backed_iceberg_proxy", 0.0) or 0.0)
            dom_fragmentation_provider_count = int(micro.get("dom_fragmentation_provider_count", 0) or 0)
            dom_cross_venue_mid_dislocation_bps = float(micro.get("dom_cross_venue_mid_dislocation_bps", 0.0) or 0.0)
            dom_cross_venue_agreement = float(micro.get("dom_cross_venue_agreement", 0.0) or 0.0)
            dom_primary_vs_consensus_gap = float(micro.get("dom_primary_vs_consensus_gap", 0.0) or 0.0)
            dom_fragmentation_score = float(micro.get("dom_fragmentation_score", 0.0) or 0.0)
            dom_fragmented_market = bool(micro.get("dom_fragmented_market"))
            dom_ladder_ready = bool(micro.get("dom_ladder_ready"))
            dom_stream_connected = bool(micro.get("dom_stream_connected"))
            dom_stream_health_known = bool(micro.get("dom_stream_health_known"))
            dom_stream_degraded = bool(micro.get("dom_stream_degraded"))
            dom_stream_health_score = float(micro.get("dom_stream_health_score", 1.0) or 1.0)
            dom_stream_trust_decay = float(micro.get("dom_stream_trust_decay", 0.0) or 0.0)
            dom_stream_reconnect_count = int(micro.get("dom_stream_reconnect_count", 0) or 0)
            dom_stream_sequence_gap_count = int(micro.get("dom_stream_sequence_gap_count", 0) or 0)
            dom_stream_last_message_age_seconds = (
                round(float(micro.get("dom_stream_last_message_age_seconds")), 3)
                if micro.get("dom_stream_last_message_age_seconds") not in (None, "")
                else None
            )
            dom_depth_stream_age_seconds = (
                round(float(micro.get("dom_depth_stream_age_seconds")), 3)
                if micro.get("dom_depth_stream_age_seconds") not in (None, "")
                else None
            )
            dom_trade_stream_age_seconds = (
                round(float(micro.get("dom_trade_stream_age_seconds")), 3)
                if micro.get("dom_trade_stream_age_seconds") not in (None, "")
                else None
            )
            dom_depth_stream_missing = bool(micro.get("dom_depth_stream_missing"))
            dom_trade_stream_missing = bool(micro.get("dom_trade_stream_missing"))
            dom_stream_reason = str(micro.get("dom_stream_reason") or "")
            aligned_micro = micro_score if signal.direction == "BUY" else -micro_score
            aligned_book = book_imbalance if signal.direction == "BUY" else -book_imbalance
            aligned_tick = tick_imbalance if signal.direction == "BUY" else -tick_imbalance
            aligned_trade_flow = trade_flow_score if signal.direction == "BUY" else -trade_flow_score
            aligned_velocity = velocity_bps if signal.direction == "BUY" else -velocity_bps
            aligned_dom_liquidity_shift = dom_liquidity_shift_proxy if signal.direction == "BUY" else -dom_liquidity_shift_proxy
            aligned_dom_sweep_pressure = dom_sweep_pressure_proxy if signal.direction == "BUY" else -dom_sweep_pressure_proxy
            aligned_dom_refill_resilience = dom_refill_resilience_proxy if signal.direction == "BUY" else -dom_refill_resilience_proxy
            aligned_dom_absorption = dom_absorption_proxy if signal.direction == "BUY" else -dom_absorption_proxy
            aligned_dom_iceberg = dom_iceberg_proxy if signal.direction == "BUY" else -dom_iceberg_proxy
            aligned_dom_add_intent = dom_add_intent_bias if signal.direction == "BUY" else -dom_add_intent_bias
            aligned_dom_cancel_pressure = dom_cancel_pressure_bias if signal.direction == "BUY" else -dom_cancel_pressure_bias
            aligned_dom_queue_erosion = dom_queue_erosion_bias if signal.direction == "BUY" else -dom_queue_erosion_bias
            aligned_dom_trade_absorption = dom_trade_absorption_proxy if signal.direction == "BUY" else -dom_trade_absorption_proxy
            aligned_dom_refill_after_sweep = dom_refill_after_sweep_bias if signal.direction == "BUY" else -dom_refill_after_sweep_bias
            aligned_dom_trade_aggression = dom_trade_aggression_bias if signal.direction == "BUY" else -dom_trade_aggression_bias
            aligned_dom_trade_backed_iceberg = (
                dom_trade_backed_iceberg_proxy if signal.direction == "BUY" else -dom_trade_backed_iceberg_proxy
            )
            signal.metadata["market_microstructure"] = dict(micro)
            signal.metadata["microstructure_score"] = round(micro_score, 3)
            signal.metadata["stop_hunt_risk"] = round(stop_hunt_risk, 3)
            signal.metadata["exhaustion_risk"] = round(exhaustion_risk, 3)
            signal.metadata["microstructure_alignment"] = round(aligned_micro, 3)
            signal.metadata["tick_imbalance"] = round(tick_imbalance, 4)
            signal.metadata["book_imbalance"] = round(book_imbalance, 4)
            signal.metadata["trade_flow_score"] = round(trade_flow_score, 4)
            signal.metadata["trade_delta_ratio"] = round(trade_delta_ratio, 4)
            signal.metadata["trade_cvd_slope"] = round(trade_cvd_slope, 4)
            signal.metadata["velocity_bps"] = round(velocity_bps, 4)
            signal.metadata["depth_available"] = bool(micro.get("depth_available"))
            signal.metadata["synthetic_depth_available"] = bool(micro.get("synthetic_depth_available"))
            signal.metadata["depth_levels"] = int(micro.get("depth_levels", 0) or 0)
            signal.metadata["bid_level_count"] = int(micro.get("bid_level_count", micro.get("visible_bid_levels", 0)) or 0)
            signal.metadata["ask_level_count"] = int(micro.get("ask_level_count", micro.get("visible_ask_levels", 0)) or 0)
            signal.metadata["depth_quality"] = round(float(micro.get("depth_quality", 0.0) or 0.0), 4)
            signal.metadata["depth_quality_tier"] = str(micro.get("depth_quality_tier") or "")
            signal.metadata["depth_provider"] = str(micro.get("depth_provider") or micro.get("source") or "")
            signal.metadata["depth_provider_class"] = str(micro.get("depth_provider_class", micro.get("source_class", "")) or "")
            signal.metadata["depth_environment"] = str(micro.get("depth_environment", micro.get("environment", "")) or "")
            signal.metadata["depth_provider_trust_score"] = round(float(micro.get("depth_provider_trust_score", 0.0) or 0.0), 4)
            signal.metadata["depth_quote_agreement_state"] = str(micro.get("depth_quote_agreement_state") or "")
            signal.metadata["depth_quote_agreement_bps"] = (
                round(float(micro.get("depth_quote_agreement_bps")), 4)
                if micro.get("depth_quote_agreement_bps") not in (None, "")
                else None
            )
            signal.metadata["depth_quote_alignment_score"] = round(float(micro.get("depth_quote_alignment_score", 0.0) or 0.0), 4)
            signal.metadata["external_depth_rejected"] = bool(micro.get("external_depth_rejected"))
            signal.metadata["external_depth_rejection_reason"] = str(micro.get("external_depth_rejection_reason") or "")
            signal.metadata["microstructure_source"] = str(micro.get("microstructure_source") or "")
            signal.metadata["depth_update_mode"] = str(micro.get("depth_update_mode") or "")
            signal.metadata["dom_event_backed"] = bool(micro.get("dom_event_backed"))
            signal.metadata["dom_ladder_ready"] = bool(micro.get("dom_ladder_ready"))
            signal.metadata["dom_stream_snapshot_ready"] = bool(micro.get("dom_stream_snapshot_ready"))
            signal.metadata["dom_depth_event_age_seconds"] = (
                round(float(micro.get("dom_depth_event_age_seconds")), 3)
                if micro.get("dom_depth_event_age_seconds") not in (None, "")
                else None
            )
            signal.metadata["dom_snapshot_span_seconds"] = (
                round(float(micro.get("dom_snapshot_span_seconds")), 3)
                if micro.get("dom_snapshot_span_seconds") not in (None, "")
                else None
            )
            signal.metadata["dom_snapshot_count"] = int(micro.get("dom_snapshot_count", 0) or 0)
            signal.metadata["dom_delta_count"] = int(micro.get("dom_delta_count", 0) or 0)
            signal.metadata["dom_trade_count"] = int(micro.get("dom_trade_count", 0) or 0)
            signal.metadata["dom_source_fidelity"] = str(micro.get("dom_source_fidelity") or "")
            signal.metadata["dom_authority_tier"] = str(micro.get("dom_authority_tier") or "")
            signal.metadata["dom_liquidity_shift_proxy"] = round(dom_liquidity_shift_proxy, 4)
            signal.metadata["dom_sweep_pressure_proxy"] = round(dom_sweep_pressure_proxy, 4)
            signal.metadata["dom_refill_resilience_proxy"] = round(dom_refill_resilience_proxy, 4)
            signal.metadata["dom_absorption_proxy"] = round(dom_absorption_proxy, 4)
            signal.metadata["dom_iceberg_proxy"] = round(dom_iceberg_proxy, 4)
            signal.metadata["dom_queue_persistence"] = round(dom_queue_persistence, 4)
            signal.metadata["dom_add_intent_bias"] = round(dom_add_intent_bias, 4)
            signal.metadata["dom_cancel_pressure_bias"] = round(dom_cancel_pressure_bias, 4)
            signal.metadata["dom_queue_erosion_bias"] = round(dom_queue_erosion_bias, 4)
            signal.metadata["dom_trade_absorption_proxy"] = round(dom_trade_absorption_proxy, 4)
            signal.metadata["dom_refill_after_sweep_bias"] = round(dom_refill_after_sweep_bias, 4)
            signal.metadata["dom_trade_aggression_bias"] = round(dom_trade_aggression_bias, 4)
            signal.metadata["dom_trade_backed_iceberg_proxy"] = round(dom_trade_backed_iceberg_proxy, 4)
            signal.metadata["dom_trade_backed_iceberg_hits"] = int(micro.get("dom_trade_backed_iceberg_hits", 0) or 0)
            signal.metadata["dom_refill_after_sweep_hits"] = int(micro.get("dom_refill_after_sweep_hits", 0) or 0)
            signal.metadata["dom_sweep_up_count"] = int(micro.get("dom_sweep_up_count", 0) or 0)
            signal.metadata["dom_sweep_down_count"] = int(micro.get("dom_sweep_down_count", 0) or 0)
            signal.metadata["dom_fragmentation_provider_count"] = dom_fragmentation_provider_count
            signal.metadata["dom_cross_venue_mid_dislocation_bps"] = round(dom_cross_venue_mid_dislocation_bps, 4)
            signal.metadata["dom_cross_venue_agreement"] = round(dom_cross_venue_agreement, 4)
            signal.metadata["dom_primary_vs_consensus_gap"] = round(dom_primary_vs_consensus_gap, 4)
            signal.metadata["dom_fragmentation_score"] = round(dom_fragmentation_score, 4)
            signal.metadata["dom_fragmented_market"] = dom_fragmented_market
            signal.metadata["dom_stream_connected"] = dom_stream_connected
            signal.metadata["dom_stream_health_known"] = dom_stream_health_known
            signal.metadata["dom_stream_degraded"] = dom_stream_degraded
            signal.metadata["dom_stream_health_score"] = round(dom_stream_health_score, 4)
            signal.metadata["dom_stream_trust_decay"] = round(dom_stream_trust_decay, 4)
            signal.metadata["dom_stream_reconnect_count"] = dom_stream_reconnect_count
            signal.metadata["dom_stream_sequence_gap_count"] = dom_stream_sequence_gap_count
            signal.metadata["dom_stream_last_message_age_seconds"] = dom_stream_last_message_age_seconds
            signal.metadata["dom_depth_stream_age_seconds"] = dom_depth_stream_age_seconds
            signal.metadata["dom_trade_stream_age_seconds"] = dom_trade_stream_age_seconds
            signal.metadata["dom_depth_stream_missing"] = dom_depth_stream_missing
            signal.metadata["dom_trade_stream_missing"] = dom_trade_stream_missing
            signal.metadata["dom_stream_reason"] = dom_stream_reason
            signal.metadata["dom_depth_window"] = int(micro.get("dom_depth_window", 0) or 0)
            signal.metadata["dom_supportive_reload_count"] = int(micro.get("dom_supportive_reload_count", 0) or 0)
            data["microstructure_score"] = round(micro_score, 3)
            data["stop_hunt_risk"] = round(stop_hunt_risk, 3)
            data["exhaustion_risk"] = round(exhaustion_risk, 3)
            data["tick_imbalance"] = round(tick_imbalance, 4)
            data["book_imbalance"] = round(book_imbalance, 4)
            data["trade_flow_score"] = round(trade_flow_score, 4)
            data["trade_delta_ratio"] = round(trade_delta_ratio, 4)
            data["trade_cvd_slope"] = round(trade_cvd_slope, 4)
            data["velocity_bps"] = round(velocity_bps, 4)
            data["synthetic_depth_available"] = bool(micro.get("synthetic_depth_available"))
            data["depth_levels"] = int(micro.get("depth_levels", 0) or 0)
            data["depth_quality"] = round(float(micro.get("depth_quality", 0.0) or 0.0), 4)
            data["depth_provider"] = str(micro.get("depth_provider") or micro.get("source") or "")
            data["depth_environment"] = str(micro.get("depth_environment", micro.get("environment", "")) or "")
            data["depth_provider_trust_score"] = round(float(micro.get("depth_provider_trust_score", 0.0) or 0.0), 4)
            data["depth_quote_agreement_state"] = str(micro.get("depth_quote_agreement_state") or "")
            data["depth_quote_agreement_bps"] = (
                round(float(micro.get("depth_quote_agreement_bps")), 4)
                if micro.get("depth_quote_agreement_bps") not in (None, "")
                else None
            )
            data["external_depth_rejected"] = bool(micro.get("external_depth_rejected"))
            data["depth_update_mode"] = str(micro.get("depth_update_mode") or "")
            data["dom_event_backed"] = bool(micro.get("dom_event_backed"))
            data["dom_ladder_ready"] = bool(micro.get("dom_ladder_ready"))
            data["dom_stream_snapshot_ready"] = bool(micro.get("dom_stream_snapshot_ready"))
            data["dom_depth_event_age_seconds"] = (
                round(float(micro.get("dom_depth_event_age_seconds")), 3)
                if micro.get("dom_depth_event_age_seconds") not in (None, "")
                else None
            )
            data["dom_snapshot_span_seconds"] = (
                round(float(micro.get("dom_snapshot_span_seconds")), 3)
                if micro.get("dom_snapshot_span_seconds") not in (None, "")
                else None
            )
            data["dom_source_fidelity"] = str(micro.get("dom_source_fidelity") or "")
            data["dom_liquidity_shift_proxy"] = round(dom_liquidity_shift_proxy, 4)
            data["dom_sweep_pressure_proxy"] = round(dom_sweep_pressure_proxy, 4)
            data["dom_refill_resilience_proxy"] = round(dom_refill_resilience_proxy, 4)
            data["dom_absorption_proxy"] = round(dom_absorption_proxy, 4)
            data["dom_iceberg_proxy"] = round(dom_iceberg_proxy, 4)
            data["dom_queue_persistence"] = round(dom_queue_persistence, 4)
            data["dom_add_intent_bias"] = round(dom_add_intent_bias, 4)
            data["dom_cancel_pressure_bias"] = round(dom_cancel_pressure_bias, 4)
            data["dom_queue_erosion_bias"] = round(dom_queue_erosion_bias, 4)
            data["dom_trade_absorption_proxy"] = round(dom_trade_absorption_proxy, 4)
            data["dom_refill_after_sweep_bias"] = round(dom_refill_after_sweep_bias, 4)
            data["dom_trade_aggression_bias"] = round(dom_trade_aggression_bias, 4)
            data["dom_trade_backed_iceberg_proxy"] = round(dom_trade_backed_iceberg_proxy, 4)
            data["dom_trade_backed_iceberg_hits"] = int(micro.get("dom_trade_backed_iceberg_hits", 0) or 0)
            data["dom_refill_after_sweep_hits"] = int(micro.get("dom_refill_after_sweep_hits", 0) or 0)
            data["dom_sweep_up_count"] = int(micro.get("dom_sweep_up_count", 0) or 0)
            data["dom_sweep_down_count"] = int(micro.get("dom_sweep_down_count", 0) or 0)
            data["dom_fragmentation_provider_count"] = dom_fragmentation_provider_count
            data["dom_cross_venue_mid_dislocation_bps"] = round(dom_cross_venue_mid_dislocation_bps, 4)
            data["dom_cross_venue_agreement"] = round(dom_cross_venue_agreement, 4)
            data["dom_primary_vs_consensus_gap"] = round(dom_primary_vs_consensus_gap, 4)
            data["dom_fragmentation_score"] = round(dom_fragmentation_score, 4)
            data["dom_fragmented_market"] = dom_fragmented_market
            data["dom_stream_connected"] = dom_stream_connected
            data["dom_stream_health_known"] = dom_stream_health_known
            data["dom_stream_degraded"] = dom_stream_degraded
            data["dom_stream_health_score"] = round(dom_stream_health_score, 4)
            data["dom_stream_trust_decay"] = round(dom_stream_trust_decay, 4)
            data["dom_stream_reconnect_count"] = dom_stream_reconnect_count
            data["dom_stream_sequence_gap_count"] = dom_stream_sequence_gap_count
            data["dom_stream_last_message_age_seconds"] = dom_stream_last_message_age_seconds
            data["dom_depth_stream_age_seconds"] = dom_depth_stream_age_seconds
            data["dom_trade_stream_age_seconds"] = dom_trade_stream_age_seconds
            data["dom_depth_stream_missing"] = dom_depth_stream_missing
            data["dom_trade_stream_missing"] = dom_trade_stream_missing
            data["dom_stream_reason"] = dom_stream_reason
            if stop_hunt_risk >= 0.45:
                notes.append("stop_hunt_penalty")
            if exhaustion_risk >= 0.42:
                notes.append("micro_exhaustion")
            if micro.get("synthetic_depth_available"):
                notes.append("synthetic_depth_proxy")
            if micro.get("external_depth_rejected"):
                notes.append("depth_quote_divergence")
            if dom_stream_health_known and dom_stream_degraded:
                notes.append("dom_stream_degraded")
            if dom_ladder_ready and dom_depth_stream_missing:
                notes.append("dom_depth_stream_gap")
            if dom_ladder_ready and dom_trade_stream_missing:
                notes.append("dom_trade_stream_gap")
            if aligned_micro >= 0.20:
                notes.append("micro_boost")
            elif aligned_micro <= -0.20:
                notes.append("micro_penalty")
            if aligned_book >= 0.18:
                notes.append("book_pressure_support")
            elif aligned_book <= -0.18:
                notes.append("book_pressure_conflict")
            if aligned_trade_flow >= 0.18:
                notes.append("trade_flow_support")
            elif aligned_trade_flow <= -0.18:
                notes.append("trade_flow_conflict")
            if aligned_tick >= 0.22 and aligned_velocity > 0:
                notes.append("micro_momentum_support")
            elif aligned_tick <= -0.22 and aligned_velocity < 0:
                notes.append("micro_momentum_conflict")
            if bool(micro.get("dom_stream_snapshot_ready")):
                if max(
                    aligned_dom_liquidity_shift,
                    aligned_dom_sweep_pressure,
                    aligned_dom_refill_resilience,
                    aligned_dom_absorption,
                    aligned_dom_iceberg,
                ) >= 0.16:
                    notes.append("snapshot_stream_support")
                elif min(
                    aligned_dom_liquidity_shift,
                    aligned_dom_sweep_pressure,
                    aligned_dom_refill_resilience,
                    aligned_dom_absorption,
                    aligned_dom_iceberg,
                ) <= -0.16:
                    notes.append("snapshot_stream_conflict")
            if aligned_dom_add_intent >= 0.14 and aligned_dom_cancel_pressure >= 0.12:
                notes.append("dom_add_cancel_support")
            elif aligned_dom_add_intent <= -0.14 and aligned_dom_cancel_pressure <= -0.12:
                notes.append("dom_add_cancel_conflict")
            if aligned_dom_trade_absorption >= 0.12 and aligned_dom_refill_after_sweep >= 0.10:
                notes.append("dom_absorption_refill_support")
            elif aligned_dom_trade_absorption <= -0.12 and aligned_dom_refill_after_sweep <= -0.10:
                notes.append("dom_absorption_refill_conflict")
            if aligned_dom_trade_backed_iceberg >= 0.14:
                notes.append("dom_trade_backed_iceberg_support")
            elif aligned_dom_trade_backed_iceberg <= -0.14:
                notes.append("dom_trade_backed_iceberg_conflict")
            if dom_fragmented_market and dom_fragmentation_score >= 0.42:
                notes.append("dom_fragmented_market")
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
        session_quality_label = str(structure.get("session_quality_label", "unknown") or "unknown")
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
        session_anchor_type = str(structure.get("session_anchor_type", "") or "")
        session_anchor_label = str(structure.get("session_anchor_label", "") or "")
        session_anchor_state = str(structure.get("session_anchor_state", "unavailable") or "unavailable")
        session_anchor_bias = str(structure.get("session_anchor_bias", "neutral") or "neutral")
        session_anchor_support_score = float(structure.get("session_anchor_support_score", 0.0) or 0.0)
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
        signal.metadata["session_quality_label"] = session_quality_label
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
        signal.metadata["session_anchor_type"] = session_anchor_type
        signal.metadata["session_anchor_label"] = session_anchor_label
        signal.metadata["session_anchor_state"] = session_anchor_state
        signal.metadata["session_anchor_bias"] = session_anchor_bias
        signal.metadata["session_anchor_support_score"] = round(session_anchor_support_score, 4)
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
            "session_quality_label": session_quality_label,
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
            "session_anchor_type": session_anchor_type,
            "session_anchor_label": session_anchor_label,
            "session_anchor_state": session_anchor_state,
            "session_anchor_bias": session_anchor_bias,
            "session_anchor_support_score": round(session_anchor_support_score, 4),
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
        if session_anchor_support_score >= 0.20:
            notes.append("session_anchor_support")
        elif session_anchor_support_score <= -0.20:
            notes.append("session_anchor_conflict")
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

    @staticmethod
    def _market_post_news_guard(
        signal: Signal,
        *,
        news_state: str,
        event_name: str,
        impact: str,
        news_direction: str,
        mins: int,
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        if news_state != "post" or impact not in {"HIGH", "MEDIUM"}:
            return ""

        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        is_news_followthrough = playbook_name == "news_impulse" or entry_style == "news_followthrough"
        aligned_with_news = bool(news_direction) and str(news_direction).upper() == str(signal.direction).upper()

        guard = {
            "state": news_state,
            "impact": impact,
            "event": event_name,
            "mins_since_event": int(mins),
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "is_news_followthrough": is_news_followthrough,
            "aligned_with_news": aligned_with_news,
            "action": "none",
        }
        signal.metadata["post_news_guard"] = dict(guard)
        data["post_news_guard"] = dict(guard)

        if impact == "HIGH":
            if mins <= 15:
                if not aligned_with_news:
                    guard["action"] = "block_conflict"
                    signal.metadata["post_news_guard"] = dict(guard)
                    data["post_news_guard"] = dict(guard)
                    return f"post-HIGH-impact direction conflicts with trade: {event_name}"
                if not is_news_followthrough:
                    guard["action"] = "block_generic"
                    signal.metadata["post_news_guard"] = dict(guard)
                    data["post_news_guard"] = dict(guard)
                    return f"generic entry is too early after HIGH impact event: {event_name}"
                notes.append("post_high_news_followthrough")
                guard["action"] = "allow_news_followthrough"
                signal.metadata["post_news_guard"] = dict(guard)
                data["post_news_guard"] = dict(guard)
                return ""

            if mins <= 30:
                if news_direction and not aligned_with_news:
                    guard["action"] = "block_conflict"
                    signal.metadata["post_news_guard"] = dict(guard)
                    data["post_news_guard"] = dict(guard)
                    return f"post-HIGH-impact direction conflicts with trade during cooldown: {event_name}"
                if not is_news_followthrough:
                    penalty = 0.03 if signal.category in {"forex", "commodities", "indices"} else 0.02
                    signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
                    signal.metadata["news_cooldown_penalty"] = penalty
                    notes.append("post_high_event_cooldown")
                    guard["action"] = "reduce_generic"
                    guard["confidence_penalty"] = penalty
                    signal.metadata["post_news_guard"] = dict(guard)
                    data["post_news_guard"] = dict(guard)
                else:
                    notes.append("post_high_news_followthrough")
                    guard["action"] = "allow_news_followthrough"
                    signal.metadata["post_news_guard"] = dict(guard)
                    data["post_news_guard"] = dict(guard)
                return ""

        if impact == "MEDIUM" and mins <= 15 and news_direction and not aligned_with_news:
            penalty = 0.015 if signal.category in {"forex", "commodities", "indices"} else 0.01
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            signal.metadata["news_cooldown_penalty"] = penalty
            notes.append("post_medium_event_conflict")
            guard["action"] = "reduce_conflict"
            guard["confidence_penalty"] = penalty
            signal.metadata["post_news_guard"] = dict(guard)
            data["post_news_guard"] = dict(guard)
        return ""

    @staticmethod
    def _market_higher_timeframe_guard(
        signal: Signal,
        *,
        structure: Dict[str, Any],
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        if not isinstance(structure, dict) or not structure:
            return ""

        execution_policy = get_execution_policy(signal.asset)
        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        continuation_like = any(token in entry_style for token in ("continuation", "breakout", "trend"))
        if playbook_name in {"breakout_continuation"}:
            continuation_like = True

        direction_sign = 1 if str(signal.direction).upper() == "BUY" else -1
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        flow_override_state = SignalDecisionEngine._depth_flow_override_state(
            signal,
            execution_policy=execution_policy,
        )

        def _trend_sign(value: Any) -> int:
            token = str(value or "").strip().lower()
            if token == "trending_up":
                return 1
            if token == "trending_down":
                return -1
            return 0

        trend_map = {
            "1h": _trend_sign(structure.get("trend_1h")),
            "4h": _trend_sign(structure.get("trend_4h")),
        }
        conflict_frames = [label for label, sign in trend_map.items() if sign and sign != direction_sign]
        support_frames = [label for label, sign in trend_map.items() if sign and sign == direction_sign]
        trigger_trend_sign = _trend_sign(structure.get("trend_5m"))
        reversal_evidence = bool(structure.get("failed_opposite_move_confirmed")) or (
            signal.direction == "BUY" and bool(structure.get("liquidity_sweep_sell"))
        ) or (
            signal.direction == "SELL" and bool(structure.get("liquidity_sweep_buy"))
        )
        depth_flow_override_supported = bool(
            flow_override_state["override_supported"]
            and alignment_score >= 0.56
            and setup_quality >= 0.52
            and trigger_trend_sign != -direction_sign
        )

        guard = {
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "continuation_like": continuation_like,
            "conflict_frames": list(conflict_frames),
            "support_frames": list(support_frames),
            "reversal_evidence": reversal_evidence,
            "depth_flow_override_supported": depth_flow_override_supported,
            "depth_flow_override_source": flow_override_state["override_source"],
            "directional_flow_support": flow_override_state["directional_flow_support"],
            "directional_flow_conflict": flow_override_state["directional_flow_conflict"],
            "action": "none",
        }
        signal.metadata["higher_timeframe_guard"] = dict(guard)
        data["higher_timeframe_guard"] = dict(guard)

        if len(conflict_frames) < 2 or support_frames:
            return ""

        if continuation_like and not reversal_evidence:
            if depth_flow_override_supported:
                penalty = float(execution_policy.get("htf_depth_override_penalty", 0.015) or 0.015)
                if flow_override_state["override_source"] != "true_depth":
                    penalty = max(penalty, 0.02)
                signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
                signal.metadata["higher_timeframe_conflict_penalty"] = penalty
                notes.append("htf_conflict")
                notes.append(
                    "htf_depth_override"
                    if flow_override_state["override_source"] == "true_depth"
                    else "htf_flow_override"
                )
                guard["action"] = "reduce_depth_override"
                guard["confidence_penalty"] = penalty
                signal.metadata["higher_timeframe_guard"] = dict(guard)
                data["higher_timeframe_guard"] = dict(guard)
                return ""
            guard["action"] = "block"
            signal.metadata["higher_timeframe_guard"] = dict(guard)
            data["higher_timeframe_guard"] = dict(guard)
            return (
                "higher timeframe structure is aligned against the trade "
                f"({', '.join(conflict_frames)})"
            )

        penalty = 0.03 if signal.category in {"forex", "commodities", "indices"} else 0.02
        signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
        signal.metadata["higher_timeframe_conflict_penalty"] = penalty
        notes.append("htf_conflict")
        if reversal_evidence:
            notes.append("htf_reversal_candidate")
        guard["action"] = "reduce"
        guard["confidence_penalty"] = penalty
        signal.metadata["higher_timeframe_guard"] = dict(guard)
        data["higher_timeframe_guard"] = dict(guard)
        return ""

    @staticmethod
    def _depth_flow_override_state(
        signal: Signal,
        *,
        execution_policy: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        execution_policy = execution_policy or get_execution_policy(signal.asset)
        direction_sign = 1 if str(signal.direction).upper() == "BUY" else -1

        orderflow_imbalance = float(signal.metadata.get("orderflow_imbalance", 0.0) or 0.0)
        microstructure_alignment = float(signal.metadata.get("microstructure_alignment", 0.0) or 0.0)
        trade_flow_score = float(signal.metadata.get("trade_flow_score", 0.0) or 0.0)
        aligned_trade_flow = trade_flow_score * direction_sign
        aligned_book_pressure = float(signal.metadata.get("book_imbalance", 0.0) or 0.0) * direction_sign
        aligned_tick_pressure = float(signal.metadata.get("tick_imbalance", 0.0) or 0.0) * direction_sign
        directional_flow_support = max(
            orderflow_imbalance * direction_sign,
            microstructure_alignment,
            aligned_trade_flow,
            aligned_book_pressure,
            aligned_tick_pressure,
        )
        directional_flow_conflict = min(
            orderflow_imbalance * direction_sign,
            microstructure_alignment,
            aligned_trade_flow,
            aligned_book_pressure,
            aligned_tick_pressure,
        )

        synthetic_depth_only = bool(signal.metadata.get("synthetic_depth_available")) and not bool(
            signal.metadata.get("depth_available")
        )
        depth_quality = float(signal.metadata.get("depth_quality", 0.0) or 0.0)
        depth_quality_tier = str(signal.metadata.get("depth_quality_tier", "") or "").strip().lower()
        depth_levels = int(signal.metadata.get("depth_levels", 0) or 0)
        if depth_levels <= 0:
            depth_levels = max(
                int(signal.metadata.get("bid_level_count", signal.metadata.get("visible_bid_levels", 0)) or 0),
                int(signal.metadata.get("ask_level_count", signal.metadata.get("visible_ask_levels", 0)) or 0),
            )
        if depth_levels <= 0:
            depth_levels = {
                "full": 10,
                "strong": 8,
                "solid": 6,
                "partial": 4,
                "thin": 2,
                "top_only": 1,
            }.get(depth_quality_tier, 0)

        microstructure_source = str(signal.metadata.get("microstructure_source", "") or "").strip().lower()
        depth_update_mode = str(signal.metadata.get("depth_update_mode", "") or "").strip().lower()
        dom_event_backed = bool(signal.metadata.get("dom_event_backed"))
        dom_ladder_ready = bool(signal.metadata.get("dom_ladder_ready"))
        dom_stream_snapshot_ready = bool(signal.metadata.get("dom_stream_snapshot_ready"))
        dom_source_fidelity = str(signal.metadata.get("dom_source_fidelity", "") or "").strip().lower()
        depth_provider = str(
            signal.metadata.get("depth_provider")
            or signal.metadata.get("provider")
            or signal.metadata.get("source")
            or signal.metadata.get("exchange")
            or ""
        ).strip().lower()
        depth_provider_class = str(
            signal.metadata.get("depth_provider_class") or signal.metadata.get("source_class") or ""
        ).strip().lower()
        depth_environment = str(signal.metadata.get("depth_environment", "") or "").strip().lower()
        depth_provider_trust_score = float(signal.metadata.get("depth_provider_trust_score", 0.0) or 0.0)
        if depth_provider_trust_score <= 0.0 and microstructure_source == "order_flow_true_depth":
            depth_provider_trust_score = 0.90
        elif depth_provider_trust_score <= 0.0 and (
            depth_provider_class == "exchange_depth"
            or any(token in depth_provider for token in ("binance", "bybit", "okx"))
        ):
            depth_provider_trust_score = 0.88
        elif depth_provider_trust_score <= 0.0 and "dukascopy" in depth_provider:
            depth_provider_trust_score = 0.92
        elif depth_provider_trust_score <= 0.0 and "ctrader" in depth_provider:
            depth_provider_trust_score = 0.58 if depth_environment and depth_environment != "live" else 0.78
        elif depth_provider_trust_score <= 0.0 and depth_provider_class == "redis_subscriber":
            depth_provider_trust_score = 0.90

        depth_quote_agreement_state = str(signal.metadata.get("depth_quote_agreement_state", "") or "").strip().lower()
        depth_quote_alignment_score = float(signal.metadata.get("depth_quote_alignment_score", 0.0) or 0.0)
        external_depth_rejected = bool(signal.metadata.get("external_depth_rejected"))
        preferred_true_depth = _is_preferred_true_depth_source(
            microstructure_source,
            depth_provider,
            depth_provider_class,
        )
        true_depth_available = bool(signal.metadata.get("depth_available")) and not synthetic_depth_only
        preferred_true_depth_min_quality = float(
            execution_policy.get("preferred_true_depth_min_quality", 0.50) or 0.50
        )
        preferred_true_depth_min_trust_score = float(
            execution_policy.get("preferred_true_depth_min_trust_score", 0.78) or 0.78
        )
        snapshot_true_depth_min_levels = int(
            execution_policy.get("snapshot_true_depth_min_levels", 50) or 50
        )
        depth_sovereignty_min_directional_flow = float(
            execution_policy.get("depth_sovereignty_min_directional_flow", 0.22) or 0.22
        )
        depth_sovereignty_min_true_depth_support = float(
            execution_policy.get("depth_sovereignty_min_true_depth_support", 0.12) or 0.12
        )
        depth_sovereignty_min_component = float(
            execution_policy.get("depth_sovereignty_min_component", 0.18) or 0.18
        )
        dom_stream_trust_metrics = _effective_dom_stream_trust_metrics(
            signal,
            execution_policy,
            base_trust_score=depth_provider_trust_score,
        )
        effective_depth_provider_trust_score = float(
            dom_stream_trust_metrics["effective_trust_score"]
        )
        true_depth_quote_aligned = bool(
            not external_depth_rejected
            and depth_quote_agreement_state not in {"divergent", "severe_divergence"}
        )
        trusted_real_dom_book_available = _trusted_real_dom_book_available(
            metadata=signal.metadata,
            true_depth_available=true_depth_available,
            preferred_true_depth=preferred_true_depth,
            microstructure_source=microstructure_source,
            depth_provider=depth_provider,
            depth_provider_class=depth_provider_class,
            depth_update_mode=depth_update_mode,
            depth_levels=depth_levels,
            snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
            depth_quality=depth_quality,
            min_quality=preferred_true_depth_min_quality,
            raw_depth_trust_score=depth_provider_trust_score,
            min_trust_score=preferred_true_depth_min_trust_score,
            depth_quote_alignment_score=depth_quote_alignment_score,
            true_depth_quote_aligned=true_depth_quote_aligned,
        )
        true_depth_informative = bool(
            true_depth_available
            and preferred_true_depth
            and depth_levels >= 2
            and depth_quality >= preferred_true_depth_min_quality
            and (
                effective_depth_provider_trust_score >= preferred_true_depth_min_trust_score
                or trusted_real_dom_book_available
            )
            and depth_quote_alignment_score >= 0.80
            and true_depth_quote_aligned
            and (
                not dom_ladder_ready
                or dom_stream_trust_metrics["sovereignty_supported"]
                or trusted_real_dom_book_available
            )
        )
        snapshot_true_depth_informative = bool(
            true_depth_informative
            and (not dom_ladder_ready or trusted_real_dom_book_available)
        )
        trusted_snapshot_true_depth_available = bool(
            snapshot_true_depth_informative
            and _trusted_snapshot_true_depth_source(
                microstructure_source=microstructure_source,
                depth_provider=depth_provider,
                depth_provider_class=depth_provider_class,
                depth_levels=depth_levels,
                snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
            )
            and depth_update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}
            and depth_quality >= preferred_true_depth_min_quality
            and (
                effective_depth_provider_trust_score >= preferred_true_depth_min_trust_score
                or trusted_real_dom_book_available
            )
            and depth_quote_alignment_score >= 0.80
        )
        trusted_real_dom_fallback_available = bool(
            trusted_real_dom_book_available
            and (
                not dom_ladder_ready
                or bool(dom_stream_trust_metrics["sovereignty_supported"])
                or dom_stream_snapshot_ready
                or depth_update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}
                or bool(dom_stream_trust_metrics["health_known"])
            )
        )
        strong_true_depth_support = bool(
            (true_depth_informative and (dom_ladder_ready or trusted_snapshot_true_depth_available))
            or trusted_real_dom_fallback_available
        ) and bool(
            directional_flow_support >= depth_sovereignty_min_directional_flow
            and aligned_book_pressure >= depth_sovereignty_min_true_depth_support
            and directional_flow_conflict > -0.10
        )
        strong_flow_support = bool(
            directional_flow_support >= depth_sovereignty_min_directional_flow
            and directional_flow_conflict > -0.12
            and max(
                microstructure_alignment,
                aligned_trade_flow,
                aligned_book_pressure,
                aligned_tick_pressure,
            )
            >= depth_sovereignty_min_component
        )
        override_source = "true_depth" if strong_true_depth_support else "flow" if strong_flow_support else ""
        return {
            "directional_flow_support": round(directional_flow_support, 4),
            "directional_flow_conflict": round(directional_flow_conflict, 4),
            "strong_true_depth_support": strong_true_depth_support,
            "strong_flow_support": strong_flow_support,
            "true_depth_informative": true_depth_informative,
            "snapshot_true_depth_informative": snapshot_true_depth_informative,
            "trusted_snapshot_true_depth_available": trusted_snapshot_true_depth_available,
            "trusted_real_dom_book_available": trusted_real_dom_book_available,
            "trusted_real_dom_fallback_available": trusted_real_dom_fallback_available,
            "depth_provider_trust_score_effective": round(effective_depth_provider_trust_score, 4),
            "depth_provider_trust_decay_applied": round(
                max(0.0, depth_provider_trust_score - effective_depth_provider_trust_score),
                4,
            ),
            "dom_stream_health_known": bool(dom_stream_trust_metrics["health_known"]),
            "dom_stream_health_score": round(float(dom_stream_trust_metrics["health_score"]), 4),
            "dom_stream_trust_decay": round(float(dom_stream_trust_metrics["trust_decay"]), 4),
            "dom_stream_degraded": bool(dom_stream_trust_metrics["stream_degraded"]),
            "depth_stream_health_hard_floor_breached": bool(
                dom_stream_trust_metrics["hard_floor_breached"]
            ),
            "dom_event_backed": dom_event_backed,
            "dom_ladder_ready": dom_ladder_ready,
            "dom_stream_snapshot_ready": dom_stream_snapshot_ready,
            "dom_source_fidelity": dom_source_fidelity or ("event_ladder" if dom_ladder_ready else "snapshot_depth" if true_depth_available else "none"),
            "depth_update_mode": depth_update_mode or ("event_stream" if dom_event_backed else ""),
            "override_supported": bool(override_source),
            "override_source": override_source,
        }

    @staticmethod
    def _market_open_spike_guard(
        signal: Signal,
        *,
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        asset = str(signal.canonical_asset or signal.asset or "").strip()
        if not asset:
            return ""

        try:
            from services.market_hours_guard import open_spike_status

            spike = open_spike_status(asset, signal.category)
        except Exception:
            return ""

        if not isinstance(spike, dict) or not spike:
            return ""

        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        combined = f"{playbook_name} {entry_style}"
        continuation_like = any(token in combined for token in ("continuation", "breakout", "trend"))
        open_specialist = any(token in combined for token in ("opening", "open_drive", "opening_range", "orb"))
        generic_flow_override = bool(signal.metadata.get("generic_flow_override"))
        generic_flow_override_source = str(signal.metadata.get("generic_flow_override_source") or "").strip().lower()
        seed_score = float(signal.metadata.get("seed_candidate_score", signal.confidence) or signal.confidence or 0.0)
        alignment_score = float(signal.metadata.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(signal.metadata.get("setup_quality", 0.0) or 0.0)
        playbook_context_confluence = float(signal.metadata.get("playbook_context_confluence", 0.0) or 0.0)
        playbook_cross_alignment = float(signal.metadata.get("playbook_cross_alignment", 0.0) or 0.0)
        playbook_micro_score = float(signal.metadata.get("playbook_micro_score", 0.0) or 0.0)
        support_components = int(signal.metadata.get("playbook_support_components", 0) or 0)
        conflict_components = int(signal.metadata.get("playbook_conflict_components", 0) or 0)
        depth_like_generic_override = generic_flow_override_source in {"true_depth", "snapshot_depth"}
        open_flow_override_supported = bool(
            continuation_like
            and generic_flow_override
            and support_components >= (1 if generic_flow_override_source == "true_depth" else 2)
            and conflict_components == 0
            and seed_score >= (
                0.68
                if generic_flow_override_source == "true_depth"
                else 0.70
                if generic_flow_override_source == "snapshot_depth"
                else 0.72
            )
            and alignment_score >= 0.58
            and setup_quality >= 0.56
            and max(
                playbook_context_confluence,
                playbook_cross_alignment,
                playbook_micro_score,
            )
            >= (
                0.18
                if generic_flow_override_source == "true_depth"
                else 0.20
                if generic_flow_override_source == "snapshot_depth"
                else 0.22
            )
        )

        guard = {
            "active": bool(spike.get("active")),
            "category": str(signal.category or ""),
            "market": str(spike.get("market") or ""),
            "label": str(spike.get("label") or ""),
            "reason": str(spike.get("reason") or ""),
            "minutes_since_open": spike.get("minutes_since_open"),
            "window_minutes": int(spike.get("window_minutes") or 0),
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "continuation_like": continuation_like,
            "open_specialist": open_specialist,
            "generic_flow_override": generic_flow_override,
            "generic_flow_override_source": generic_flow_override_source,
            "open_flow_override_supported": open_flow_override_supported,
            "support_components": support_components,
            "conflict_components": conflict_components,
            "action": "none",
        }
        signal.metadata["open_spike_guard"] = dict(guard)
        data["open_spike_guard"] = dict(guard)

        if not guard["active"]:
            return ""

        label = guard["label"] or "market open"

        if signal.category == "indices":
            if open_specialist:
                notes.append("open_spike_specialist")
                guard["action"] = "allow_open_specialist"
                signal.metadata["open_spike_guard"] = dict(guard)
                data["open_spike_guard"] = dict(guard)
                return ""

            if continuation_like and open_flow_override_supported:
                if generic_flow_override_source == "true_depth":
                    notes.append("open_spike_depth_override")
                    guard["action"] = "allow_depth_override"
                    signal.metadata["open_spike_guard"] = dict(guard)
                    data["open_spike_guard"] = dict(guard)
                    return ""

                penalty = 0.01 if generic_flow_override_source == "snapshot_depth" else 0.02
                signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
                signal.metadata["open_spike_penalty"] = penalty
                notes.append("open_spike_snapshot_override" if depth_like_generic_override else "open_spike_flow_override")
                guard["action"] = "reduce_snapshot_override" if depth_like_generic_override else "reduce_flow_override"
                guard["confidence_penalty"] = penalty
                signal.metadata["open_spike_guard"] = dict(guard)
                data["open_spike_guard"] = dict(guard)
                return ""

            if continuation_like:
                guard["action"] = "block_generic_continuation"
                signal.metadata["open_spike_guard"] = dict(guard)
                data["open_spike_guard"] = dict(guard)
                return f"generic continuation is too early after {label}"

            penalty = 0.03
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            signal.metadata["open_spike_penalty"] = penalty
            notes.append("index_open_spike_cooldown")
            guard["action"] = "reduce_generic"
            guard["confidence_penalty"] = penalty
            signal.metadata["open_spike_guard"] = dict(guard)
            data["open_spike_guard"] = dict(guard)
            return ""

        if signal.category == "commodities":
            if open_specialist:
                notes.append("open_spike_specialist")
                guard["action"] = "allow_open_specialist"
                signal.metadata["open_spike_guard"] = dict(guard)
                data["open_spike_guard"] = dict(guard)
                return ""

            penalty = 0.03 if continuation_like else 0.02
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            signal.metadata["open_spike_penalty"] = penalty
            notes.append("commodity_reopen_cooldown")
            guard["action"] = "reduce_generic"
            guard["confidence_penalty"] = penalty
            signal.metadata["open_spike_guard"] = dict(guard)
            data["open_spike_guard"] = dict(guard)
            return ""

        return ""

    @staticmethod
    def _market_session_anchor_guard(
        signal: Signal,
        *,
        structure: Dict[str, Any],
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        if not isinstance(structure, dict) or not structure:
            return ""

        anchor_type = str(structure.get("session_anchor_type") or "").strip().lower()
        anchor_label = str(structure.get("session_anchor_label") or anchor_type or "session anchor").strip()
        anchor_state = str(structure.get("session_anchor_state") or "unavailable").strip().lower()
        anchor_bias = str(structure.get("session_anchor_bias") or "neutral").strip().lower()
        anchor_ready = bool(structure.get("session_anchor_ready"))
        support_score = float(structure.get("session_anchor_support_score", 0.0) or 0.0)

        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        combined = f"{playbook_name} {entry_style}"
        continuation_like = any(token in combined for token in ("continuation", "breakout", "trend", "pullback"))

        guard = {
            "anchor_type": anchor_type,
            "anchor_label": anchor_label,
            "anchor_state": anchor_state,
            "anchor_bias": anchor_bias,
            "anchor_ready": anchor_ready,
            "support_score": round(support_score, 4),
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "continuation_like": continuation_like,
            "action": "none",
        }
        signal.metadata["session_anchor_guard"] = dict(guard)
        data["session_anchor_guard"] = dict(guard)

        if not anchor_type or not anchor_ready or abs(support_score) < 0.20:
            return ""

        if continuation_like and support_score <= -0.55:
            guard["action"] = "block"
            signal.metadata["session_anchor_guard"] = dict(guard)
            data["session_anchor_guard"] = dict(guard)
            return f"{anchor_label} is rejecting the trade"

        if support_score <= -0.20:
            penalty = 0.03 if signal.category in {"forex", "indices"} else 0.02
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            signal.metadata["session_anchor_penalty"] = penalty
            notes.append("session_anchor_conflict_penalty")
            if "failed_" in anchor_state:
                notes.append("session_anchor_failed_break")
            guard["action"] = "reduce"
            guard["confidence_penalty"] = penalty
            signal.metadata["session_anchor_guard"] = dict(guard)
            data["session_anchor_guard"] = dict(guard)
        return ""

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

        post_news_block_reason = self._market_post_news_guard(
            signal,
            news_state=news_state,
            event_name=event_name,
            impact=impact,
            news_direction=_news_direction,
            mins=mins,
            data=data,
            notes=notes,
        )
        if post_news_block_reason:
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=post_news_block_reason,
                conf_before=conf_before,
                data=data,
            )

        open_spike_block_reason = self._market_open_spike_guard(
            signal,
            data=data,
            notes=notes,
        )
        if open_spike_block_reason:
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=open_spike_block_reason,
                conf_before=conf_before,
                data=data,
            )

        session_anchor_block_reason = self._market_session_anchor_guard(
            signal,
            structure=structure,
            data=data,
            notes=notes,
        )
        if session_anchor_block_reason:
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=session_anchor_block_reason,
                conf_before=conf_before,
                data=data,
            )

        htf_block_reason = self._market_higher_timeframe_guard(
            signal,
            structure=structure,
            data=data,
            notes=notes,
        )
        if htf_block_reason:
            return self._kill_review(
                signal,
                step=STEP_MARKET,
                name="market",
                reason=htf_block_reason,
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
    def _intelligence_crypto_major_guard(
        signal: Signal,
        *,
        cross_asset: Dict[str, Any],
        funding_bias: str,
        oi_signal: str,
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        category = str(signal.category or "").strip().lower()
        canonical_asset = str(signal.canonical_asset or signal.asset or "").strip().upper()
        is_major = canonical_asset.startswith("BTC") or canonical_asset.startswith("ETH")
        dominant_peer = str(
            cross_asset.get("dominant_peer")
            or signal.metadata.get("cross_asset_primary_peer")
            or ""
        ).strip().upper()
        dominant_relation = str(
            cross_asset.get("dominant_relation")
            or signal.metadata.get("cross_asset_primary_relation")
            or ""
        ).strip().lower()
        supportive_direction = _normalize_trade_direction_label(
            cross_asset.get("supportive_direction")
            or signal.metadata.get("cross_asset_supportive_direction")
        )
        cross_alignment = float(
            cross_asset.get("alignment", signal.metadata.get("cross_asset_alignment", 0.0)) or 0.0
        )
        cross_confidence = float(
            cross_asset.get("confidence", signal.metadata.get("cross_asset_confidence", 0.0)) or 0.0
        )
        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        continuation_like = any(token in f"{playbook_name} {entry_style}" for token in ("continuation", "breakout", "trend"))

        direction_sign = 1 if str(signal.direction).upper() == "BUY" else -1
        orderflow_imbalance = float(signal.metadata.get("orderflow_imbalance", 0.0) or 0.0)
        microstructure_alignment = float(signal.metadata.get("microstructure_alignment", 0.0) or 0.0)
        book_imbalance = float(signal.metadata.get("book_imbalance", 0.0) or 0.0)
        tick_imbalance = float(signal.metadata.get("tick_imbalance", 0.0) or 0.0)
        trade_flow_score = float(signal.metadata.get("trade_flow_score", 0.0) or 0.0)
        aligned_orderflow = orderflow_imbalance * direction_sign
        aligned_book = book_imbalance * direction_sign
        aligned_tick = tick_imbalance * direction_sign
        aligned_trade_flow = trade_flow_score * direction_sign
        flow_conflict_score = min(
            microstructure_alignment,
            aligned_orderflow,
            aligned_book,
            aligned_tick,
            aligned_trade_flow,
        )
        flow_support_score = max(
            microstructure_alignment,
            aligned_orderflow,
            aligned_book,
            aligned_tick,
            aligned_trade_flow,
        )
        funding_direction = _normalize_trade_direction_label(funding_bias)
        funding_supports_trade = bool(funding_direction and funding_direction == signal.direction)
        funding_conflicts_trade = bool(funding_direction and funding_direction != signal.direction)
        oi_signal_upper = str(oi_signal or "NEUTRAL").strip().upper()
        derivative_conflict = funding_conflicts_trade or (
            oi_signal_upper == "TREND_CONTINUATION"
            and supportive_direction
            and supportive_direction != signal.direction
        )
        derivative_support = funding_supports_trade or (
            oi_signal_upper == "TREND_CONTINUATION"
            and supportive_direction
            and supportive_direction == signal.direction
        )
        major_led_context = bool(
            dominant_peer in {"BTC-USD", "ETH-USD"}
            or dominant_relation in {"btc_lead", "eth_confirmation", "crypto_breadth", "alt_breadth"}
        )
        setup_quality = float(signal.metadata.get("setup_quality", 0.0) or 0.0)
        playbook_confidence = float(signal.metadata.get("playbook_confidence", 0.0) or 0.0)
        weak_setup = bool(
            float(signal.confidence) < 0.7
            or (setup_quality > 0.0 and setup_quality < 0.68)
            or (playbook_confidence > 0.0 and playbook_confidence < 0.74)
        )

        guard = {
            "active": bool(category == "crypto" and not is_major and major_led_context),
            "canonical_asset": canonical_asset,
            "dominant_peer": dominant_peer,
            "dominant_relation": dominant_relation,
            "supportive_direction": supportive_direction or "",
            "cross_alignment": round(cross_alignment, 4),
            "cross_confidence": round(cross_confidence, 4),
            "continuation_like": continuation_like,
            "aligned_orderflow": round(aligned_orderflow, 4),
            "aligned_microstructure": round(microstructure_alignment, 4),
            "aligned_book": round(aligned_book, 4),
            "aligned_tick": round(aligned_tick, 4),
            "aligned_trade_flow": round(aligned_trade_flow, 4),
            "flow_conflict_score": round(flow_conflict_score, 4),
            "flow_support_score": round(flow_support_score, 4),
            "funding_bias": str(funding_bias or "NEUTRAL"),
            "oi_signal": oi_signal_upper,
            "derivative_conflict": derivative_conflict,
            "derivative_support": derivative_support,
            "weak_setup": weak_setup,
            "action": "none",
        }
        signal.metadata["crypto_major_guard"] = dict(guard)
        data["crypto_major_guard"] = dict(guard)

        if not guard["active"]:
            return ""

        conflict = bool(
            cross_confidence >= 0.42
            and cross_alignment <= -0.28
            and (not supportive_direction or supportive_direction != signal.direction)
        )
        local_conflict = bool(flow_conflict_score <= -0.14 or derivative_conflict)
        local_support = bool(flow_support_score >= 0.14 or derivative_support)
        strong_conflict = bool(cross_confidence >= 0.55 and cross_alignment <= -0.42)

        if not conflict:
            if local_support:
                notes.append("crypto_major_support")
                guard["action"] = "allow_local_support"
                signal.metadata["crypto_major_guard"] = dict(guard)
                data["crypto_major_guard"] = dict(guard)
            return ""

        if continuation_like and weak_setup and strong_conflict and local_conflict and not local_support:
            guard["action"] = "block"
            signal.metadata["crypto_major_guard"] = dict(guard)
            data["crypto_major_guard"] = dict(guard)
            return "BTC/ETH leadership and local crypto flow are aligned against this alt setup"

        penalty = 0.035 if local_conflict and not local_support else 0.02
        signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
        signal.metadata["crypto_major_conflict_penalty"] = penalty
        notes.append("crypto_major_conflict")
        if local_support:
            notes.append("crypto_local_support")
            guard["action"] = "reduce_buffered_by_local_support"
        else:
            guard["action"] = "reduce"
        guard["confidence_penalty"] = penalty
        signal.metadata["crypto_major_guard"] = dict(guard)
        data["crypto_major_guard"] = dict(guard)
        return ""

    @staticmethod
    def _intelligence_crypto_dominance_guard(
        signal: Signal,
        *,
        data: Dict[str, Any],
        notes: List[str],
    ) -> str:
        category = str(signal.category or "").strip().lower()
        canonical_asset = str(signal.canonical_asset or signal.asset or "").strip().upper()
        if category != "crypto" or canonical_asset.startswith("BTC") or canonical_asset.startswith("ETH"):
            return ""

        details = signal.metadata.get("market_intelligence_details")
        if not isinstance(details, dict):
            return ""
        cg = details.get("coingecko_global")
        if not isinstance(cg, dict):
            return ""

        btc_dom = float(cg.get("btc_dominance", 0.0) or 0.0)
        eth_dom = float(cg.get("eth_dominance", 0.0) or 0.0)
        btc_delta = float(cg.get("btc_dominance_delta", 0.0) or 0.0)
        guard = {
            "active": True,
            "btc_dominance": round(btc_dom, 4),
            "eth_dominance": round(eth_dom, 4),
            "btc_dominance_delta": round(btc_delta, 4),
            "action": "none",
        }

        if signal.direction == "BUY" and btc_dom >= 58.0 and btc_delta >= 0.10:
            penalty = 0.025 if float(signal.confidence or 0.0) < 0.72 else 0.015
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            guard["action"] = "reduce_alt_buy"
            guard["confidence_penalty"] = penalty
            notes.append("btc_dominance_rising_against_alt_buy")
        elif signal.direction == "SELL" and btc_dom <= 52.0 and btc_delta <= -0.10:
            penalty = 0.025 if float(signal.confidence or 0.0) < 0.72 else 0.015
            signal.confidence = round(max(0.0, float(signal.confidence) - penalty), 4)
            guard["action"] = "reduce_alt_sell"
            guard["confidence_penalty"] = penalty
            notes.append("btc_dominance_falling_against_alt_sell")

        signal.metadata["crypto_dominance_guard"] = dict(guard)
        data["crypto_dominance_guard"] = dict(guard)
        return ""

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
        notes: List[str] = []
        data = {
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
        }
        crypto_major_block_reason = SignalDecisionEngine._intelligence_crypto_major_guard(
            signal,
            cross_asset=cross_asset,
            funding_bias=funding_bias,
            oi_signal=oi_signal,
            data=data,
            notes=notes,
        )
        SignalDecisionEngine._intelligence_crypto_dominance_guard(
            signal,
            data=data,
            notes=notes,
        )
        if notes:
            signal.metadata["intelligence_review_notes"] = list(notes)
            data["notes"] = list(notes)
        if crypto_major_block_reason:
            return SignalDecisionEngine._kill_review(
                signal,
                step=STEP_INTELLIGENCE,
                name="intelligence",
                reason=crypto_major_block_reason,
                conf_before=conf_before,
                data=data,
            )
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
            data=data,
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
                "asset_performance_profile": dict(adaptive_policy.get("asset_performance_profile") or {}),
                "book_performance_profile": dict(adaptive_policy.get("book_performance_profile") or {}),
                "context_protection_profile": dict(adaptive_policy.get("context_protection_profile") or {}),
                "session_performance_profile": dict(adaptive_policy.get("session_performance_profile") or {}),
                "inactivity_profile": dict(adaptive_policy.get("inactivity_profile") or {}),
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
            broker_spread_regime = str(signal.metadata.get("broker_spread_regime", "") or "").strip().lower()
            micro = signal.metadata.get("market_microstructure") if isinstance(signal.metadata.get("market_microstructure"), dict) else {}
            spread_stress = float(signal.metadata.get("spread_stress", micro.get("spread_stress", 0.0)) or 0.0)
            data["liquidity_proxy"] = round(liquidity, 6)
            data["spread_regime"] = broker_spread_regime
            data["spread_stress"] = round(spread_stress, 4)
            if broker_spread_regime in {"wide", "stressed", "extreme"}:
                return self._kill_review(
                    signal,
                    step=STEP_EXECUTION,
                    name="execution",
                    reason=f"spread regime is {broker_spread_regime}",
                    conf_before=conf_before,
                    data=data,
                )
            if spread_stress >= 1.45:
                return self._kill_review(
                    signal,
                    step=STEP_EXECUTION,
                    name="execution",
                    reason=f"spread stress {spread_stress:.2f} is too elevated",
                    conf_before=conf_before,
                    data=data,
                )
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
        structure_bias = str(structure.get("structure_bias", signal.metadata.get("structure_bias", "")) or "").strip().lower()
        setup_quality = float(structure.get("setup_quality", signal.metadata.get("setup_quality", 0.0)) or 0.0)
        vwap_distance_atr = float(structure.get("vwap_distance_atr", signal.metadata.get("vwap_distance_atr", 0.0)) or 0.0)
        session_quality_label = str(
            structure.get("session_quality_label", signal.metadata.get("session_quality_label", "unknown")) or "unknown"
        ).strip().lower()
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
        asset_performance = (
            raw_policy.get("asset_performance_profile")
            if isinstance(raw_policy, dict) and isinstance(raw_policy.get("asset_performance_profile"), dict)
            else {}
        )
        book_performance = (
            raw_policy.get("book_performance_profile")
            if isinstance(raw_policy, dict) and isinstance(raw_policy.get("book_performance_profile"), dict)
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
        inactivity_flat_book = bool(inactivity_profile.get("flat_book")) or bool(inactivity_profile.get("equity_relief"))
        inactivity_hours_since_last_entry = float(inactivity_profile.get("hours_since_last_entry", 0.0) or 0.0)
        asset_score = float(asset_performance.get("asset_score", 0.5) or 0.5)
        asset_action = str(asset_performance.get("action") or "neutral").strip().lower()
        asset_sample_count = int(asset_performance.get("sample_count", 0) or 0)
        book_score = float(book_performance.get("book_score", 0.5) or 0.5)
        book_action = str(book_performance.get("action") or "neutral").strip().lower()
        book_sample_count = int(book_performance.get("sample_count", 0) or 0)
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
        fast_entry_confirmation_bars_required = int(
            structure.get(
                "fast_entry_confirmation_bars_required",
                signal.metadata.get("fast_entry_confirmation_bars_required", 0),
            )
            or 0
        )
        fast_entry_confirmation_count = int(
            structure.get(
                "fast_entry_confirmation_count",
                signal.metadata.get("fast_entry_confirmation_count", 0),
            )
            or 0
        )
        fast_entry_confirmation_ready = bool(
            structure.get("fast_entry_confirmation_ready", signal.metadata.get("fast_entry_confirmation_ready"))
        )
        external_confirmation_score = float(
            structure.get(
                "external_confirmation_score",
                signal.metadata.get("external_confirmation_score", 0.0),
            )
            or 0.0
        )
        pattern_family = str(structure.get("pattern_family", signal.metadata.get("pattern_family", "unknown")) or "unknown")
        trend_5m = str(structure.get("trend_5m", signal.metadata.get("trend_5m", "unknown")) or "unknown").strip().lower()
        close_location = float(structure.get("close_location", signal.metadata.get("close_location", 0.5)) or 0.5)
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
        depth_quality = float(signal.metadata.get("depth_quality", 0.0) or 0.0)
        depth_quality_tier = str(signal.metadata.get("depth_quality_tier", "") or "").strip().lower()
        depth_levels = int(signal.metadata.get("depth_levels", 0) or 0)
        if depth_levels <= 0:
            depth_levels = max(
                int(signal.metadata.get("bid_level_count", signal.metadata.get("visible_bid_levels", 0)) or 0),
                int(signal.metadata.get("ask_level_count", signal.metadata.get("visible_ask_levels", 0)) or 0),
            )
        if depth_levels <= 0:
            depth_levels = {
                "full": 10,
                "strong": 8,
                "solid": 6,
                "partial": 4,
                "thin": 2,
                "top_only": 1,
            }.get(depth_quality_tier, 0)
        microstructure_source = str(signal.metadata.get("microstructure_source", "") or "").strip().lower()
        depth_provider = str(
            signal.metadata.get("depth_provider")
            or signal.metadata.get("provider")
            or signal.metadata.get("source")
            or signal.metadata.get("exchange")
            or ""
        ).strip().lower()
        depth_provider_class = str(
            signal.metadata.get("depth_provider_class") or signal.metadata.get("source_class") or ""
        ).strip().lower()
        depth_environment = str(signal.metadata.get("depth_environment", "") or "").strip().lower()
        true_depth_provider_kind = _true_depth_provider_kind(
            microstructure_source,
            depth_provider,
            depth_provider_class,
        )
        exchange_true_depth_source = true_depth_provider_kind == "exchange"
        sidecar_true_depth_source = true_depth_provider_kind == "sidecar"
        redis_true_depth_source = true_depth_provider_kind == "redis"
        depth_provider_trust_score = float(signal.metadata.get("depth_provider_trust_score", 0.0) or 0.0)
        if depth_provider_trust_score <= 0.0 and microstructure_source == "order_flow_true_depth":
            depth_provider_trust_score = 0.90
        elif depth_provider_trust_score <= 0.0 and (
            depth_provider_class == "exchange_depth"
            or any(token in depth_provider for token in ("binance", "bybit", "okx"))
        ):
            depth_provider_trust_score = 0.88
        elif depth_provider_trust_score <= 0.0 and "dukascopy" in depth_provider:
            depth_provider_trust_score = 0.92
        elif depth_provider_trust_score <= 0.0 and "ctrader" in depth_provider:
            depth_provider_trust_score = 0.58 if depth_environment and depth_environment != "live" else 0.78
        elif depth_provider_trust_score <= 0.0 and depth_provider_class == "redis_subscriber":
            depth_provider_trust_score = 0.90
        depth_quote_agreement_state = str(signal.metadata.get("depth_quote_agreement_state", "") or "").strip().lower()
        depth_quote_alignment_score = float(signal.metadata.get("depth_quote_alignment_score", 0.0) or 0.0)
        external_depth_rejected = bool(signal.metadata.get("external_depth_rejected"))
        cross_asset_alignment = float(signal.metadata.get("cross_asset_alignment", 0.0) or 0.0)
        cross_asset_confidence = float(signal.metadata.get("cross_asset_confidence", 0.0) or 0.0)
        cross_asset_supportive_direction = _normalize_trade_direction_label(
            signal.metadata.get("cross_asset_supportive_direction")
        )
        cross_asset_primary_peer = str(signal.metadata.get("cross_asset_primary_peer") or "").strip().upper()
        cross_asset_primary_relation = str(
            signal.metadata.get("cross_asset_primary_relation") or ""
        ).strip().lower()
        supportive_structure_distance = float(signal.metadata.get("supportive_structure_distance", 0.0) or 0.0)
        category_label = str(signal.category or signal.metadata.get("category") or "").strip().lower()
        canonical_asset = str(signal.canonical_asset or signal.asset or "").strip().upper()
        session_timing_strictness = _session_timing_strictness(
            category_label,
            session_quality_label,
            session_quality_score,
        )
        execution_policy = get_execution_policy(signal.asset)
        dom_stream_trust_metrics = _effective_dom_stream_trust_metrics(
            signal,
            execution_policy,
            base_trust_score=depth_provider_trust_score,
        )
        dom_stream_health_known = bool(dom_stream_trust_metrics["health_known"])
        dom_stream_health_score = float(dom_stream_trust_metrics["health_score"])
        dom_stream_trust_decay = float(dom_stream_trust_metrics["trust_decay"])
        dom_stream_degraded = bool(dom_stream_trust_metrics["stream_degraded"])
        dom_depth_stream_missing = bool(dom_stream_trust_metrics["depth_stream_missing"])
        dom_trade_stream_missing = bool(dom_stream_trust_metrics["trade_stream_missing"])
        depth_provider_trust_score_effective = float(
            dom_stream_trust_metrics["effective_trust_score"]
        )
        dom_stream_sovereignty_supported = bool(
            dom_stream_trust_metrics["sovereignty_supported"]
        )
        dom_stream_hard_floor_breached = bool(dom_stream_trust_metrics["hard_floor_breached"])
        direction_sign = 1 if signal.direction == "BUY" else -1
        funding_bias = str(signal.metadata.get("funding_bias") or "NEUTRAL").strip().upper()
        oi_signal = str(signal.metadata.get("oi_signal") or "NEUTRAL").strip().upper()
        funding_direction = _normalize_trade_direction_label(funding_bias)
        orderflow_imbalance = float(signal.metadata.get("orderflow_imbalance", 0.0) or 0.0)
        microstructure_alignment = float(signal.metadata.get("microstructure_alignment", 0.0) or 0.0)
        trade_flow_score = float(signal.metadata.get("trade_flow_score", 0.0) or 0.0)
        trade_delta_ratio = float(signal.metadata.get("trade_delta_ratio", 0.0) or 0.0)
        trade_cvd_slope = float(signal.metadata.get("trade_cvd_slope", 0.0) or 0.0)
        aligned_trade_flow = trade_flow_score * direction_sign
        aligned_trade_delta_ratio = trade_delta_ratio * direction_sign
        aligned_trade_cvd_slope = trade_cvd_slope * direction_sign
        aligned_book_pressure = float(signal.metadata.get("book_imbalance", 0.0) or 0.0) * direction_sign
        aligned_tick_pressure = float(signal.metadata.get("tick_imbalance", 0.0) or 0.0) * direction_sign
        recent_pattern_sample_count = int(recent_review.get("sample_count", 0) or 0)
        playbook_name = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or signal.metadata.get("strategy_id")
            or signal.strategy_id
            or ""
        ).strip().lower()
        entry_style = str(
            signal.metadata.get("playbook_entry_style")
            or signal.metadata.get("entry_style")
            or ""
        ).strip().lower()
        seed_score = float(
            signal.metadata.get("seed_candidate_score", signal.confidence)
            or signal.confidence
            or 0.0
        )
        playbook_context_confluence = float(
            signal.metadata.get("playbook_context_confluence", 0.0) or 0.0
        )
        playbook_cross_alignment = float(
            signal.metadata.get("playbook_cross_alignment", 0.0) or 0.0
        )
        playbook_micro_score = float(signal.metadata.get("playbook_micro_score", 0.0) or 0.0)
        playbook_support_components = int(
            signal.metadata.get("playbook_support_components", 0) or 0
        )
        playbook_conflict_components = int(
            signal.metadata.get("playbook_conflict_components", 0) or 0
        )
        impulse_break_style = bool(
            entry_style in {
                "expansion_break",
                "opening_drive_break",
                "news_followthrough",
                "intermarket_break",
                "intermarket_confirmed_break",
                "breakout_close",
            }
            or (
                "break" in entry_style
                and "pullback" not in entry_style
                and "retest" not in entry_style
            )
        )
        impulse_playbook = bool(
            playbook_name in {
                "aggressive_expansion",
                "opening_drive",
                "news_impulse",
                "intermarket_continuation",
                "breakout_continuation",
            }
        )
        shock_score = float(signal.metadata.get("shock_score", 0.0) or 0.0)
        shock_event_score = float(signal.metadata.get("shock_event_score", 0.0) or 0.0)
        shock_displacement_score = float(signal.metadata.get("shock_displacement_score", 0.0) or 0.0)
        shock_structure_score = float(signal.metadata.get("shock_structure_score", 0.0) or 0.0)
        shock_liquidity_score = float(signal.metadata.get("shock_liquidity_score", 0.0) or 0.0)
        shock_timing_score = float(signal.metadata.get("shock_timing_score", 0.0) or 0.0)
        headline_shock_score = float(signal.metadata.get("headline_shock_score", 0.0) or 0.0)
        shock_fresh_event = bool(signal.metadata.get("shock_fresh_event"))
        shock_supported = bool(signal.metadata.get("shock_supported"))
        pattern_family_lower = pattern_family.strip().lower()
        continuation_family = bool(
            "continuation" in pattern_family_lower
            or pattern_family_lower.endswith("generic")
            or pattern_family_lower.endswith("liquidity_sweep")
        )
        continuation_entry = bool(
            "continuation" in entry_style
            or "breakout" in entry_style
            or entry_style in {"breakout_close", "trend_close"}
        )
        structural_strength_score = max(
            0.0,
            min(
                1.0,
                alignment_score * 0.38
                + setup_quality * 0.32
                + candle_quality_score * 0.14
                + session_quality_score * 0.10
                + target_efficiency_score * 0.06,
            ),
        )
        directional_flow_support = max(
            orderflow_imbalance * direction_sign,
            microstructure_alignment,
            aligned_trade_flow,
            aligned_book_pressure,
            aligned_tick_pressure,
        )
        directional_flow_conflict = min(
            orderflow_imbalance * direction_sign,
            microstructure_alignment,
            aligned_trade_flow,
            aligned_book_pressure,
            aligned_tick_pressure,
        )
        has_directional_flow_support = directional_flow_support >= 0.12
        has_directional_flow_conflict = directional_flow_conflict <= -0.16
        trigger_reversal_against_trade = bool(
            (signal.direction == "BUY" and trend_5m == "trending_down")
            or (signal.direction == "SELL" and trend_5m == "trending_up")
        )
        opposing_liquidity_sweep = bool(
            (signal.direction == "BUY" and liquidity_sweep_sell)
            or (signal.direction == "SELL" and liquidity_sweep_buy)
        )
        opposing_trigger_close = bool(
            (signal.direction == "BUY" and close_location <= 0.36)
            or (signal.direction == "SELL" and close_location >= 0.64)
        )
        continuation_reclaim_evidence = (
            int(trigger_reversal_against_trade)
            + int(opposing_liquidity_sweep)
            + int(opposing_trigger_close)
            + int(has_directional_flow_conflict)
        )
        mature_continuation_profile = bool(
            extension_score >= 0.92
            or directional_extension >= 0.76
            or impulse_age_bars >= 4
            or exhaustion_risk >= 0.28
            or dominant_exhaustion >= 0.45
            or bias_exhausted
        )
        shock_supported = bool(
            shock_supported
            or (
                shock_score >= 0.60
                and shock_displacement_score >= 0.58
                and shock_structure_score >= 0.54
                and shock_timing_score >= 0.50
                and (shock_liquidity_score >= 0.42 or shock_fresh_event)
            )
            or (
                headline_shock_score >= 0.56
                and shock_displacement_score >= 0.55
                and shock_structure_score >= 0.52
                and shock_timing_score >= 0.48
                and (shock_liquidity_score >= 0.40 or shock_fresh_event)
            )
        )
        continuation_reclaim_pressure = bool(
            (continuation_family or continuation_entry)
            and mature_continuation_profile
            and continuation_reclaim_evidence >= 2
        )
        is_crypto = category_label == "crypto"
        is_crypto_major = canonical_asset in {"BTC-USD", "ETH-USD"}
        is_crypto_alt = is_crypto and not is_crypto_major
        crypto_breadth_relation = bool(
            cross_asset_primary_peer in {"BTC-USD", "ETH-USD"}
            or cross_asset_primary_relation
            in {"btc_lead", "eth_confirmation", "crypto_breadth", "alt_breadth"}
        )
        crypto_breadth_conflict = bool(
            is_crypto
            and crypto_breadth_relation
            and cross_asset_confidence >= (0.30 if is_crypto_major else 0.36)
            and cross_asset_alignment <= (-0.24 if is_crypto_major else -0.32)
            and cross_asset_supportive_direction in {"BUY", "SELL"}
            and cross_asset_supportive_direction != signal.direction
        )
        crypto_breadth_support = bool(
            is_crypto
            and crypto_breadth_relation
            and cross_asset_confidence >= (0.30 if is_crypto_major else 0.36)
            and cross_asset_alignment >= (0.24 if is_crypto_major else 0.32)
            and cross_asset_supportive_direction == signal.direction
        )
        cross_asset_directional_conflict = bool(
            cross_asset_confidence >= 0.24
            and cross_asset_alignment <= -0.20
            and (
                not cross_asset_supportive_direction
                or cross_asset_supportive_direction != signal.direction
            )
        )
        strong_cross_asset_directional_conflict = bool(
            cross_asset_confidence >= 0.36
            and cross_asset_alignment <= -0.32
            and (
                not cross_asset_supportive_direction
                or cross_asset_supportive_direction != signal.direction
            )
        )
        funding_supports_trade = bool(funding_direction and funding_direction == signal.direction)
        funding_conflicts_trade = bool(funding_direction and funding_direction != signal.direction)
        oi_trend_continuation = oi_signal == "TREND_CONTINUATION"
        oi_potential_reversal = oi_signal == "POTENTIAL_REVERSAL"
        crypto_derivative_conflict = bool(
            is_crypto
            and (
                funding_conflicts_trade
                or (oi_trend_continuation and crypto_breadth_conflict)
                or (oi_potential_reversal and (continuation_family or continuation_entry))
            )
        )
        crypto_derivative_support = bool(
            is_crypto
            and (
                funding_supports_trade
                or (oi_trend_continuation and crypto_breadth_support)
            )
        )
        crypto_flow_breadth_hard_block = bool(
            crypto_breadth_conflict
            and has_directional_flow_conflict
            and (
                is_crypto_alt
                or (is_crypto_major and (continuation_family or continuation_entry) and mature_continuation_profile)
            )
        )
        strong_market_candidate = bool(
            category_label in {"crypto", "forex", "commodities", "indices"}
            and (
                (
                    float(signal.confidence or 0.0) >= 0.64
                    and alignment_score >= 0.68
                    and setup_quality >= 0.62
                    and candle_quality_score >= 0.36
                    and session_quality_score >= 0.40
                )
                or (
                    structural_strength_score >= 0.56
                    and alignment_score >= 0.74
                    and setup_quality >= 0.64
                    and candle_quality_score >= 0.30
                    and session_quality_score >= 0.36
                )
            )
        )
        shock_market_candidate = bool(
            strong_market_candidate
            or (
                category_label in {"crypto", "forex", "commodities", "indices"}
                and (
                    (
                        float(signal.confidence or 0.0) >= 0.60
                        and alignment_score >= 0.62
                        and setup_quality >= 0.56
                        and candle_quality_score >= 0.28
                        and session_quality_score >= 0.32
                    )
                    or (
                        structural_strength_score >= 0.52
                        and alignment_score >= 0.68
                        and setup_quality >= 0.58
                        and candle_quality_score >= 0.26
                        and session_quality_score >= 0.30
                    )
                )
            )
        )
        strong_fx_crypto_candidate = bool(
            category_label in {"crypto", "forex"} and strong_market_candidate
        )
        continuation_rescue_candidate = bool(
            strong_market_candidate
            and (continuation_family or continuation_entry)
            and alignment_score >= 0.76
            and setup_quality >= 0.64
            and candle_quality_score >= 0.30
            and session_quality_score >= 0.36
            and target_efficiency_score >= 0.12
            and impulse_age_bars <= 7
            and not failed_opposite_move_confirmed
            and not has_directional_flow_conflict
            and (
                has_directional_flow_support
                or entry_confirmation_ready
                or breakout_retest_ready
                or first_pullback_ready
                or elite_pattern_rank >= 0.08
            )
        )
        high_conviction_continuation_candidate = bool(
            strong_market_candidate
            and (continuation_family or continuation_entry)
            and float(signal.confidence or 0.0) >= 0.67
            and alignment_score >= 0.78
            and setup_quality >= 0.68
            and candle_quality_score >= 0.34
            and session_quality_score >= 0.40
            and target_efficiency_score >= 0.12
            and extension_score <= 1.60
            and impulse_age_bars <= 7
            and not failed_opposite_move_confirmed
            and not has_directional_flow_conflict
        )
        high_conviction_continuation_timing_intact = bool(
            high_conviction_continuation_candidate
            and extension_score <= 1.18
            and abs(vwap_distance_atr) <= 1.20
            and target_efficiency_score >= 0.16
            and impulse_age_bars <= 6
        )
        high_conviction_continuation_supported = bool(
            high_conviction_continuation_timing_intact
            and (
                elite_pattern_rank >= 0.22
                or has_directional_flow_support
                or entry_confirmation_ready
                or breakout_retest_ready
                or first_pullback_ready
            )
        )
        context_continuation_execution_candidate = bool(
            strong_market_candidate
            and (continuation_family or continuation_entry)
            and alignment_score >= 0.60
            and setup_quality >= 0.56
            and target_efficiency_score >= 0.10
            and impulse_age_bars <= 7
            and not failed_opposite_move_confirmed
            and not has_directional_flow_conflict
            and has_directional_flow_support
            and (
                external_confirmation_score >= 0.16
                or cross_asset_confidence >= 0.20
                or abs(microstructure_alignment) >= 0.22
                or abs(aligned_trade_flow) >= 0.18
                or shock_liquidity_score >= 0.56
            )
        )
        context_confirmation_override = bool(
            context_continuation_execution_candidate
            and (
                fast_entry_confirmation_ready
                or directional_flow_support >= 0.24
                or external_confirmation_score >= 0.18
                or shock_liquidity_score >= 0.60
            )
        )
        impulse_fast_path_candidate = bool(
            impulse_playbook
            and impulse_break_style
            and strong_market_candidate
            and float(signal.confidence or 0.0) >= 0.63
            and alignment_score >= 0.68
            and setup_quality >= 0.60
            and candle_quality_score >= 0.32
            and session_quality_score >= 0.38
            and target_efficiency_score >= 0.12
            and extension_score <= 1.24
            and impulse_age_bars <= 5
            and dominant_exhaustion <= 0.58
            and not failed_opposite_move_confirmed
            and not has_directional_flow_conflict
        )
        impulse_fast_path_timing_intact = bool(
            impulse_fast_path_candidate
            and extension_score <= 1.10
            and abs(vwap_distance_atr) <= 1.20
            and target_efficiency_score >= 0.14
            and impulse_age_bars <= 4
        )
        impulse_fast_path_supported = bool(
            impulse_fast_path_timing_intact
            and (
                fast_entry_confirmation_ready
                or entry_confirmation_ready
                or has_directional_flow_support
                or external_confirmation_score >= 0.18
                or (
                    category_label in {"commodities", "indices"}
                    and candle_quality_score >= 0.36
                    and session_quality_score >= 0.42
                )
            )
        )
        shock_fast_path_candidate = bool(
            impulse_playbook
            and shock_market_candidate
            and shock_supported
            and shock_score >= 0.60
            and shock_displacement_score >= 0.58
            and shock_structure_score >= 0.54
            and shock_timing_score >= 0.50
            and candle_quality_score >= 0.28
            and session_quality_score >= 0.32
            and target_efficiency_score >= 0.10
            and extension_score <= 1.32
            and impulse_age_bars <= 6
            and dominant_exhaustion <= 0.62
            and not failed_opposite_move_confirmed
            and not has_directional_flow_conflict
        )
        shock_fast_path_timing_intact = bool(
            shock_fast_path_candidate
            and extension_score <= 1.16
            and abs(vwap_distance_atr) <= 1.30
            and target_efficiency_score >= 0.12
            and impulse_age_bars <= 5
        )
        shock_confirmation_override = bool(
            shock_fast_path_timing_intact
            and (
                shock_fresh_event
                or shock_event_score >= 0.48
                or headline_shock_score >= 0.56
                or shock_liquidity_score >= 0.54
                or shock_displacement_score >= 0.70
            )
        )
        shock_fast_path_supported = bool(
            shock_fast_path_timing_intact
            and broker_spread_regime not in {"stressed", "extreme"}
            and broker_quote_quality_state not in {"stale", "delayed"}
            and (
                shock_liquidity_score >= 0.42
                or has_directional_flow_support
                or external_confirmation_score >= 0.16
                or shock_fresh_event
            )
        )
        inactivity_execution_relief = bool(
            inactivity_relief_active
            and inactivity_flat_book
            and strong_market_candidate
            and float(signal.confidence or 0.0) >= 0.68
        )

        base_risk_kill_threshold = float(execution_policy.get("risk_kill_threshold", 0.58) or 0.58)
        base_weak_candle_extension_limit = float(
            execution_policy.get("weak_candle_extension_limit", 1.25) or 1.25
        )
        base_weak_candle_floor = float(execution_policy.get("weak_candle_floor", 0.26) or 0.26)
        base_target_efficiency_hard_floor = float(
            execution_policy.get("target_efficiency_hard_floor", 0.15) or 0.15
        )
        base_opposing_distance_hard_floor = float(
            execution_policy.get("opposing_distance_hard_floor", 0.0035) or 0.0035
        )
        base_impulse_age_hard_limit = int(execution_policy.get("impulse_age_hard_limit", 6) or 6)
        base_directional_extension_hard_limit = float(
            execution_policy.get("directional_extension_hard_limit", 0.74) or 0.74
        )
        base_pattern_rank_hard_floor = float(
            execution_policy.get("pattern_rank_hard_floor", 0.12) or 0.12
        )
        base_pattern_rank_strong_floor = float(
            execution_policy.get("pattern_rank_strong_floor", 0.08) or 0.08
        )
        preferred_true_depth_min_quality = float(
            execution_policy.get("preferred_true_depth_min_quality", 0.50) or 0.50
        )
        minimum_usable_true_depth_quality = float(
            execution_policy.get("minimum_usable_true_depth_quality", 0.0) or 0.0
        )
        preferred_true_depth_min_trust_score = float(
            execution_policy.get("preferred_true_depth_min_trust_score", 0.78) or 0.78
        )
        snapshot_true_depth_min_levels = int(
            execution_policy.get("snapshot_true_depth_min_levels", 50) or 50
        )
        minimum_usable_true_depth_trust_score = float(
            execution_policy.get("minimum_usable_true_depth_trust_score", 0.60) or 0.60
        )
        if sidecar_true_depth_source:
            ctrader_sidecar_depth = bool(
                "ctrader" in depth_provider or "ctrader" in microstructure_source
            )
            sidecar_trust_floor = (
                0.58
                if ctrader_sidecar_depth
                and depth_environment not in {"", "live", "real", "production"}
                else 0.60
            )
            preferred_true_depth_min_quality = min(preferred_true_depth_min_quality, 0.25)
            preferred_true_depth_min_trust_score = min(
                preferred_true_depth_min_trust_score,
                sidecar_trust_floor,
            )
            minimum_usable_true_depth_trust_score = min(
                minimum_usable_true_depth_trust_score,
                sidecar_trust_floor,
            )
        elif exchange_true_depth_source and category_label == "crypto":
            preferred_true_depth_min_trust_score = min(
                preferred_true_depth_min_trust_score,
                0.72,
            )
        depth_sovereignty_min_directional_flow = float(
            execution_policy.get("depth_sovereignty_min_directional_flow", 0.22) or 0.22
        )
        depth_sovereignty_min_true_depth_support = float(
            execution_policy.get("depth_sovereignty_min_true_depth_support", 0.12) or 0.12
        )
        depth_sovereignty_min_component = float(
            execution_policy.get("depth_sovereignty_min_component", 0.18) or 0.18
        )
        guarded_force_entry_env = str(os.getenv("FORCE_ENTRY_MODE", "") or "").strip().lower()
        guarded_force_entry_mode = guarded_force_entry_env or (
            "live_guarded"
            if float(execution_policy.get("guarded_force_entry_enabled", 0) or 0) > 0
            else "off"
        )
        guarded_force_entry_enabled = guarded_force_entry_mode in {
            "1",
            "true",
            "on",
            "paper",
            "guarded",
            "live_guarded",
        }
        guarded_force_min_directional_flow = float(
            execution_policy.get("guarded_force_min_directional_flow", 0.34) or 0.34
        )
        guarded_force_min_book_pressure = float(
            execution_policy.get("guarded_force_min_book_pressure", 0.24) or 0.24
        )
        guarded_force_min_alignment = float(
            execution_policy.get("guarded_force_min_alignment", 0.50) or 0.50
        )
        guarded_force_min_setup_quality = float(
            execution_policy.get("guarded_force_min_setup_quality", 0.48) or 0.48
        )
        guarded_force_min_target_efficiency = float(
            execution_policy.get("guarded_force_min_target_efficiency", 0.12) or 0.12
        )
        guarded_force_max_extension_score = float(
            execution_policy.get("guarded_force_max_extension_score", 1.24) or 1.24
        )
        guarded_force_max_directional_extension = float(
            execution_policy.get("guarded_force_max_directional_extension", 0.84) or 0.84
        )
        guarded_force_max_stop_hunt_risk = float(
            execution_policy.get("guarded_force_max_stop_hunt_risk", 0.48) or 0.48
        )
        guarded_force_risk_relief = float(
            execution_policy.get("guarded_force_risk_relief", 0.18) or 0.18
        )
        asset_edge_bonus_scale = float(execution_policy.get("asset_edge_bonus_scale", 0.08) or 0.08)
        asset_edge_penalty_scale = float(execution_policy.get("asset_edge_penalty_scale", 0.09) or 0.09)
        book_edge_bonus_scale = float(execution_policy.get("book_edge_bonus_scale", 0.06) or 0.06)
        book_edge_penalty_scale = float(execution_policy.get("book_edge_penalty_scale", 0.07) or 0.07)
        thin_true_depth_penalty_scale = float(
            execution_policy.get("thin_true_depth_penalty", 0.0) or 0.0
        )
        low_trust_true_depth_penalty_scale = float(
            execution_policy.get("low_trust_true_depth_penalty", 0.0) or 0.0
        )
        misaligned_true_depth_penalty_scale = float(
            execution_policy.get("misaligned_true_depth_penalty", 0.0) or 0.0
        )

        true_depth_available = bool(signal.metadata.get("depth_available")) and not synthetic_depth_only
        preferred_true_depth = _is_preferred_true_depth_source(
            microstructure_source,
            depth_provider,
            depth_provider_class,
        )
        depth_update_mode = str(signal.metadata.get("depth_update_mode", "") or "").strip().lower()
        dom_event_backed = bool(signal.metadata.get("dom_event_backed"))
        dom_ladder_ready = bool(signal.metadata.get("dom_ladder_ready"))
        dom_stream_snapshot_ready = bool(signal.metadata.get("dom_stream_snapshot_ready"))
        dom_source_fidelity = str(signal.metadata.get("dom_source_fidelity", "") or "").strip().lower()
        dom_liquidity_shift_proxy = float(signal.metadata.get("dom_liquidity_shift_proxy", 0.0) or 0.0)
        dom_sweep_pressure_proxy = float(signal.metadata.get("dom_sweep_pressure_proxy", 0.0) or 0.0)
        dom_refill_resilience_proxy = float(signal.metadata.get("dom_refill_resilience_proxy", 0.0) or 0.0)
        dom_absorption_proxy = float(signal.metadata.get("dom_absorption_proxy", 0.0) or 0.0)
        dom_iceberg_proxy = float(signal.metadata.get("dom_iceberg_proxy", 0.0) or 0.0)
        dom_queue_persistence = float(signal.metadata.get("dom_queue_persistence", 0.0) or 0.0)
        dom_add_intent_bias = float(signal.metadata.get("dom_add_intent_bias", 0.0) or 0.0)
        dom_cancel_pressure_bias = float(signal.metadata.get("dom_cancel_pressure_bias", 0.0) or 0.0)
        dom_queue_erosion_bias = float(signal.metadata.get("dom_queue_erosion_bias", 0.0) or 0.0)
        dom_trade_absorption_proxy = float(signal.metadata.get("dom_trade_absorption_proxy", 0.0) or 0.0)
        dom_refill_after_sweep_bias = float(signal.metadata.get("dom_refill_after_sweep_bias", 0.0) or 0.0)
        dom_trade_aggression_bias = float(signal.metadata.get("dom_trade_aggression_bias", 0.0) or 0.0)
        dom_trade_backed_iceberg_proxy = float(signal.metadata.get("dom_trade_backed_iceberg_proxy", 0.0) or 0.0)
        dom_fragmentation_score = float(signal.metadata.get("dom_fragmentation_score", 0.0) or 0.0)
        dom_fragmented_market = bool(signal.metadata.get("dom_fragmented_market"))
        true_depth_quote_aligned = bool(
            not external_depth_rejected
            and depth_quote_agreement_state not in {"divergent", "severe_divergence"}
        )
        aligned_dom_add_intent = dom_add_intent_bias if signal.direction == "BUY" else -dom_add_intent_bias
        aligned_dom_cancel_pressure = dom_cancel_pressure_bias if signal.direction == "BUY" else -dom_cancel_pressure_bias
        aligned_dom_queue_erosion = dom_queue_erosion_bias if signal.direction == "BUY" else -dom_queue_erosion_bias
        aligned_dom_trade_absorption = dom_trade_absorption_proxy if signal.direction == "BUY" else -dom_trade_absorption_proxy
        aligned_dom_refill_after_sweep = dom_refill_after_sweep_bias if signal.direction == "BUY" else -dom_refill_after_sweep_bias
        aligned_dom_trade_aggression = dom_trade_aggression_bias if signal.direction == "BUY" else -dom_trade_aggression_bias
        aligned_dom_trade_backed_iceberg = (
            dom_trade_backed_iceberg_proxy if signal.direction == "BUY" else -dom_trade_backed_iceberg_proxy
        )
        depth_fragmentation_untrusted = bool(
            dom_fragmented_market
            and (
                dom_fragmentation_score >= 0.52
                or float(signal.metadata.get("dom_primary_vs_consensus_gap", 0.0) or 0.0) >= 0.24
            )
        )
        trusted_real_dom_book_available = _trusted_real_dom_book_available(
            metadata=signal.metadata,
            true_depth_available=true_depth_available,
            preferred_true_depth=preferred_true_depth,
            microstructure_source=microstructure_source,
            depth_provider=depth_provider,
            depth_provider_class=depth_provider_class,
            depth_update_mode=depth_update_mode,
            depth_levels=depth_levels,
            snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
            depth_quality=depth_quality,
            min_quality=preferred_true_depth_min_quality,
            raw_depth_trust_score=depth_provider_trust_score,
            min_trust_score=preferred_true_depth_min_trust_score,
            depth_quote_alignment_score=depth_quote_alignment_score,
            true_depth_quote_aligned=true_depth_quote_aligned,
            depth_fragmentation_untrusted=depth_fragmentation_untrusted,
        )
        meets_true_depth_trust_floor = bool(
            true_depth_available
            and (
                minimum_usable_true_depth_trust_score <= 0.0
                or depth_provider_trust_score_effective >= minimum_usable_true_depth_trust_score
                or (
                    trusted_real_dom_book_available
                    and depth_provider_trust_score >= minimum_usable_true_depth_trust_score
                )
            )
        )
        meets_true_depth_quality_floor = bool(
            true_depth_available
            and (
                minimum_usable_true_depth_quality <= 0.0
                or depth_quality >= minimum_usable_true_depth_quality
            )
        )
        true_depth_signal_strength = max(abs(aligned_book_pressure), abs(aligned_tick_pressure))
        true_depth_directional_support = max(aligned_book_pressure, aligned_tick_pressure)
        true_depth_directional_conflict = min(aligned_book_pressure, aligned_tick_pressure)
        true_depth_informative = bool(
            meets_true_depth_quality_floor
            and meets_true_depth_trust_floor
            and true_depth_quote_aligned
            and depth_levels >= 2
            and true_depth_signal_strength >= 0.06
            and (
                not dom_ladder_ready
                or dom_stream_sovereignty_supported
                or trusted_real_dom_book_available
            )
        )
        usable_true_depth_available = bool(
            true_depth_available
            and true_depth_informative
        )
        event_backed_true_depth_available = bool(
            usable_true_depth_available
            and dom_ladder_ready
            and dom_stream_sovereignty_supported
        )
        snapshot_true_depth_available = bool(
            usable_true_depth_available
            and (
                not dom_ladder_ready
                or (
                    trusted_real_dom_book_available
                    and not dom_stream_sovereignty_supported
                )
            )
        )
        trusted_snapshot_true_depth_available = bool(
            snapshot_true_depth_available
            and _trusted_snapshot_true_depth_source(
                microstructure_source=microstructure_source,
                depth_provider=depth_provider,
                depth_provider_class=depth_provider_class,
                depth_levels=depth_levels,
                snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
            )
            and depth_update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}
            and depth_quality >= preferred_true_depth_min_quality
            and (
                depth_provider_trust_score_effective >= preferred_true_depth_min_trust_score
                or trusted_real_dom_book_available
            )
            and depth_quote_alignment_score >= 0.80
            and not depth_fragmentation_untrusted
        )
        trusted_real_dom_fallback_available = bool(
            trusted_real_dom_book_available
            and (
                not dom_ladder_ready
                or dom_stream_sovereignty_supported
                or dom_stream_snapshot_ready
                or depth_update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}
                or dom_stream_health_known
            )
        )
        thin_true_depth_untrusted = bool(
            true_depth_available and preferred_true_depth and not meets_true_depth_quality_floor
        )
        low_trust_true_depth_available = bool(
            true_depth_available
            and preferred_true_depth
            and not meets_true_depth_trust_floor
        )
        misaligned_true_depth_available = bool(
            true_depth_available
            and preferred_true_depth
            and not true_depth_quote_aligned
        )
        uninformative_true_depth_available = bool(
            true_depth_available
            and preferred_true_depth
            and meets_true_depth_quality_floor
            and meets_true_depth_trust_floor
            and true_depth_quote_aligned
            and not true_depth_informative
        )
        strong_true_depth_support = bool(
            (
                event_backed_true_depth_available
                or trusted_snapshot_true_depth_available
                or trusted_real_dom_fallback_available
            )
            and preferred_true_depth
            and depth_quality >= preferred_true_depth_min_quality
            and (
                depth_provider_trust_score_effective >= preferred_true_depth_min_trust_score
                or (
                    trusted_real_dom_fallback_available
                    and depth_provider_trust_score >= preferred_true_depth_min_trust_score
                )
            )
            and depth_quote_alignment_score >= 0.80
            and directional_flow_support >= depth_sovereignty_min_directional_flow
            and aligned_book_pressure >= depth_sovereignty_min_true_depth_support
            and directional_flow_conflict > -0.10
            and not depth_fragmentation_untrusted
        )
        strong_flow_support = bool(
            directional_flow_support >= depth_sovereignty_min_directional_flow
            and directional_flow_conflict > -0.12
            and max(
                microstructure_alignment,
                aligned_trade_flow,
                aligned_book_pressure,
                aligned_tick_pressure,
            )
            >= depth_sovereignty_min_component
        )
        aligned_snapshot_stream_proxy = max(
            dom_liquidity_shift_proxy,
            dom_sweep_pressure_proxy,
            dom_refill_resilience_proxy,
            dom_absorption_proxy,
            dom_iceberg_proxy,
            aligned_dom_trade_backed_iceberg,
            aligned_dom_add_intent,
            aligned_dom_cancel_pressure,
            aligned_dom_trade_absorption,
            aligned_dom_refill_after_sweep,
            aligned_dom_trade_aggression,
            aligned_dom_queue_erosion,
        )
        snapshot_stream_supportive = bool(
            snapshot_true_depth_available
            and dom_stream_snapshot_ready
            and aligned_snapshot_stream_proxy >= 0.12
            and dom_queue_persistence >= 0.35
        )
        hostile_core_component_count = sum(
            1
            for conflict in (
                true_depth_directional_conflict <= -0.08,
                microstructure_alignment <= -0.16,
                aligned_trade_flow <= -0.16,
                aligned_trade_delta_ratio <= -0.14,
                aligned_trade_cvd_slope <= -0.10,
            )
            if conflict
        )
        hostile_ladder_detail_component_count = sum(
            1
            for conflict in (
                aligned_dom_add_intent <= -0.14,
                aligned_dom_cancel_pressure <= -0.14,
                aligned_dom_queue_erosion <= -0.12,
                aligned_dom_trade_absorption <= -0.16,
                aligned_dom_refill_after_sweep <= -0.14,
                aligned_dom_trade_aggression <= -0.14,
                aligned_dom_trade_backed_iceberg <= -0.16,
                depth_fragmentation_untrusted,
            )
            if conflict
        )
        event_ladder_hostile_flow_component_count = hostile_core_component_count + min(
            2, hostile_ladder_detail_component_count
        )
        event_ladder_hostile_flow = bool(
            event_backed_true_depth_available
            and (continuation_family or continuation_entry)
            and (
                hostile_core_component_count >= 2
                or (hostile_core_component_count >= 1 and hostile_ladder_detail_component_count >= 2)
                or hostile_ladder_detail_component_count >= 3
            )
        )
        macro_spillover_conflict_relation = bool(
            cross_asset_primary_relation in {
                "risk_on_yen",
                "gold_risk_off",
                "risk_off_equities",
                "risk_off_tech",
                "growth_cycle_confirmation",
                "global_equity_confirmation",
                "broad_equity_confirmation",
                "yen_exporter_link",
                "oil_cad_link",
                "cad_confirmation",
                "commodity_complex",
                "energy_complex",
                "usd_weakness_gold",
                "risk_on_usd_softness",
            }
            or cross_asset_primary_peer
            in {"US500", "US100", "US30", "XAU/USD", "WTI", "USD/JPY", "EUR/JPY", "GBP/JPY", "JPN225"}
        )
        event_ladder_cross_market_conflict = bool(
            (continuation_family or continuation_entry)
            and event_ladder_hostile_flow
            and cross_asset_directional_conflict
        )
        strong_event_ladder_cross_market_conflict = bool(
            event_ladder_cross_market_conflict
            and (
                strong_cross_asset_directional_conflict
                or macro_spillover_conflict_relation
                or continuation_reclaim_pressure
                or mature_continuation_profile
            )
        )
        event_ladder_cross_market_hard_block = bool(
            strong_event_ladder_cross_market_conflict
            and (
                continuation_reclaim_pressure
                or mature_continuation_profile
                or directional_extension >= 0.76
                or impulse_age_bars >= 4
            )
            and not breakout_retest_ready
            and not first_pullback_ready
            and not fast_entry_confirmation_ready
        )
        if event_ladder_cross_market_hard_block:
            continuation_rescue_candidate = False
            high_conviction_continuation_candidate = False
            high_conviction_continuation_timing_intact = False
            high_conviction_continuation_supported = False
            context_continuation_execution_candidate = False
            context_confirmation_override = False
        event_ladder_stream_health_blocks_sovereignty = bool(
            dom_ladder_ready
            and not dom_stream_sovereignty_supported
        )
        trusted_real_dom_fallback_support = bool(
            event_ladder_stream_health_blocks_sovereignty
            and trusted_real_dom_fallback_available
            and directional_flow_support >= depth_sovereignty_min_directional_flow
            and aligned_book_pressure >= depth_sovereignty_min_true_depth_support
            and directional_flow_conflict > -0.10
            and true_depth_directional_conflict > -0.08
            and not has_directional_flow_conflict
        )
        if event_ladder_stream_health_blocks_sovereignty and not trusted_real_dom_fallback_support:
            strong_true_depth_support = False
            strong_flow_support = False
        if dom_stream_hard_floor_breached and not trusted_real_dom_fallback_support:
            continuation_rescue_candidate = False
            high_conviction_continuation_candidate = False
            high_conviction_continuation_timing_intact = False
            high_conviction_continuation_supported = False
            context_continuation_execution_candidate = False
            context_confirmation_override = False
        depth_sovereignty_supported = bool(strong_true_depth_support or strong_flow_support)
        depth_sovereignty_source = "true_depth" if strong_true_depth_support else "flow" if strong_flow_support else ""
        if strong_true_depth_support:
            if trusted_real_dom_fallback_support:
                depth_sovereignty_reason = "supported:trusted_real_dom_fallback"
            elif trusted_snapshot_true_depth_available and not event_backed_true_depth_available:
                depth_sovereignty_reason = "supported:trusted_snapshot_true_depth"
            else:
                depth_sovereignty_reason = "supported:true_depth"
        elif strong_flow_support:
            depth_sovereignty_reason = "supported:flow"
        else:
            depth_sovereignty_reasons = []
            if event_ladder_stream_health_blocks_sovereignty:
                depth_sovereignty_reasons.append("dom_stream_health_blocks")
            if dom_stream_hard_floor_breached:
                depth_sovereignty_reasons.append("dom_stream_hard_floor")
            if not true_depth_available:
                depth_sovereignty_reasons.append("no_true_depth")
            elif not preferred_true_depth:
                depth_sovereignty_reasons.append("depth_source_not_preferred")
            else:
                if depth_levels < 2:
                    depth_sovereignty_reasons.append("depth_levels_too_low")
                if not meets_true_depth_quality_floor:
                    depth_sovereignty_reasons.append("depth_quality_too_low")
                if not meets_true_depth_trust_floor:
                    depth_sovereignty_reasons.append("depth_trust_too_low")
                if not true_depth_quote_aligned:
                    depth_sovereignty_reasons.append("depth_quote_misaligned")
                if depth_fragmentation_untrusted:
                    depth_sovereignty_reasons.append("depth_fragmented")
                if (
                    snapshot_true_depth_available
                    and _is_preferred_true_depth_source(
                        microstructure_source,
                        depth_provider,
                        depth_provider_class,
                    )
                    and not _trusted_snapshot_true_depth_source(
                        microstructure_source=microstructure_source,
                        depth_provider=depth_provider,
                        depth_provider_class=depth_provider_class,
                        depth_levels=depth_levels,
                        snapshot_true_depth_min_levels=snapshot_true_depth_min_levels,
                    )
                    and depth_levels < snapshot_true_depth_min_levels
                ):
                    depth_sovereignty_reasons.append("snapshot_depth_levels_below_sovereignty_floor")
                if true_depth_available and not dom_ladder_ready and not trusted_snapshot_true_depth_available:
                    depth_sovereignty_reasons.append("snapshot_depth_not_event_ladder")
                elif true_depth_available and not event_backed_true_depth_available:
                    depth_sovereignty_reasons.append("true_depth_not_event_backed")
                if aligned_book_pressure < depth_sovereignty_min_true_depth_support:
                    depth_sovereignty_reasons.append("book_pressure_weak")
            if directional_flow_support < depth_sovereignty_min_directional_flow:
                depth_sovereignty_reasons.append("flow_support_weak")
            if directional_flow_conflict <= -0.12:
                depth_sovereignty_reasons.append("flow_conflict")
            if max(
                microstructure_alignment,
                aligned_trade_flow,
                aligned_book_pressure,
                aligned_tick_pressure,
            ) < depth_sovereignty_min_component:
                depth_sovereignty_reasons.append("flow_component_weak")
            depth_sovereignty_reason = ",".join(dict.fromkeys(depth_sovereignty_reasons[:4])) or "not_supported"
        snapshot_dom_requires_confirmation = bool(
            snapshot_true_depth_available
            and not strong_true_depth_support
        )
        depth_flow_sovereignty_candidate = bool(
            depth_sovereignty_supported
            and not event_ladder_cross_market_conflict
            and not has_directional_flow_conflict
            and alignment_score >= 0.58
            and setup_quality >= 0.54
            and candle_quality_score >= 0.26
            and session_quality_score >= 0.32
            and target_efficiency_score >= 0.08
            and extension_score <= 1.28
            and impulse_age_bars <= 6
            and directional_extension <= 0.88
            and not failed_opposite_move_confirmed
            and not snapshot_dom_requires_confirmation
        )
        depth_flow_sovereignty_rescue_candidate = bool(
            depth_sovereignty_supported
            and strong_market_candidate
            and (continuation_family or continuation_entry)
            and not event_ladder_cross_market_conflict
            and not has_directional_flow_conflict
            and alignment_score >= 0.64
            and setup_quality >= 0.60
            and candle_quality_score >= 0.24
            and session_quality_score >= 0.30
            and target_efficiency_score >= 0.08
            and extension_score <= 1.42
            and impulse_age_bars <= 7
            and directional_extension <= 0.92
            and not failed_opposite_move_confirmed
            and (
                not snapshot_dom_requires_confirmation
                or external_confirmation_score >= 0.18
                or fast_entry_confirmation_ready
            )
            and (
                strong_true_depth_support
                or directional_flow_support >= max(0.30, depth_sovereignty_min_directional_flow + 0.08)
                or external_confirmation_score >= 0.18
                or fast_entry_confirmation_ready
            )
        )
        depth_flow_sovereignty_confirmation_override = bool(
            (depth_flow_sovereignty_candidate or depth_flow_sovereignty_rescue_candidate)
            and (
                strong_true_depth_support
                or (
                    not snapshot_dom_requires_confirmation
                    and directional_flow_support >= max(0.28, depth_sovereignty_min_directional_flow + 0.04)
                )
                or external_confirmation_score >= 0.16
                or fast_entry_confirmation_ready
            )
        )
        breakout_ignition_candidate = bool(
            depth_sovereignty_supported
            and impulse_break_style
            and (continuation_family or continuation_entry)
            and not event_ladder_cross_market_conflict
            and not has_directional_flow_conflict
            and not failed_opposite_move_confirmed
            and alignment_score >= 0.58
            and setup_quality >= 0.54
            and candle_quality_score >= 0.24
            and session_quality_score >= 0.30
            and target_efficiency_score >= 0.06
            and extension_score <= 1.62
            and directional_extension <= 1.10
            and impulse_age_bars <= 8
            and float(signal.metadata.get("stop_hunt_risk", 0.0) or 0.0) < 0.62
            and (
                strong_true_depth_support
                or trusted_real_dom_fallback_support
                or directional_flow_support >= max(0.32, depth_sovereignty_min_directional_flow + 0.08)
                or external_confirmation_score >= 0.18
                or fast_entry_confirmation_ready
            )
            and (
                target_efficiency_score >= 0.09
                or opposing_distance > base_opposing_distance_hard_floor
                or shock_liquidity_score >= 0.56
            )
        )
        breakout_ignition_confirmation_override = bool(
            breakout_ignition_candidate
            and (
                strong_true_depth_support
                or trusted_real_dom_fallback_support
                or directional_flow_support >= max(0.32, depth_sovereignty_min_directional_flow + 0.08)
                or external_confirmation_score >= 0.20
                or fast_entry_confirmation_ready
            )
        )
        continuation_reclaim_hard_block = bool(
            continuation_reclaim_pressure
            and not has_directional_flow_support
            and not high_conviction_continuation_supported
            and not impulse_fast_path_supported
            and not shock_fast_path_supported
            and not depth_flow_sovereignty_candidate
            and not depth_flow_sovereignty_rescue_candidate
            and not breakout_ignition_candidate
        )
        elite_supported_candidate = bool(
            strong_market_candidate
            and (
                elite_pattern_rank >= 0.16
                or failed_opposite_move_confirmed
                or breakout_retest_ready
                or first_pullback_ready
                or entry_confirmation_ready
                or fast_entry_confirmation_ready
                or high_conviction_continuation_supported
                or impulse_fast_path_supported
                or shock_fast_path_supported
                or (continuation_rescue_candidate and has_directional_flow_support)
                or depth_flow_sovereignty_candidate
                or depth_flow_sovereignty_rescue_candidate
                or breakout_ignition_candidate
            )
        )
        asset_performance_relief = 0.0
        asset_performance_penalty = 0.0
        if asset_action == "boost" and asset_sample_count > 0:
            asset_performance_relief = min(0.05, max(0.0, asset_score - 0.50) * asset_edge_bonus_scale)
        elif asset_action == "reduce" and asset_sample_count > 0:
            asset_performance_penalty = min(0.06, max(0.0, 0.50 - asset_score) * asset_edge_penalty_scale)

        book_performance_relief = 0.0
        book_performance_penalty = 0.0
        if book_action == "boost" and book_sample_count > 0:
            book_performance_relief = min(0.04, max(0.0, book_score - 0.50) * book_edge_bonus_scale)
        elif book_action == "reduce" and book_sample_count > 0:
            book_performance_penalty = min(0.05, max(0.0, 0.50 - book_score) * book_edge_penalty_scale)

        true_depth_relief = 0.0
        snapshot_depth_relief = 0.0
        synthetic_depth_penalty = 0.0
        thin_true_depth_penalty = 0.0
        low_trust_true_depth_penalty = 0.0
        misaligned_true_depth_penalty = 0.0
        if (
            event_backed_true_depth_available
            and depth_quality >= preferred_true_depth_min_quality
            and depth_provider_trust_score_effective >= preferred_true_depth_min_trust_score
            and depth_quote_alignment_score >= 0.80
            and true_depth_directional_support >= 0.06
            and true_depth_directional_conflict > -0.06
        ):
            true_depth_relief = min(
                0.08,
                float(execution_policy.get("true_depth_bonus", 0.03) or 0.03)
                + max(0.0, depth_quality - preferred_true_depth_min_quality) * 0.05,
            )
            if preferred_true_depth:
                true_depth_relief = min(0.08, true_depth_relief + 0.01)
        elif (
            snapshot_true_depth_available
            and depth_quote_alignment_score >= 0.75
            and true_depth_directional_support >= 0.06
            and true_depth_directional_conflict > -0.08
        ):
            snapshot_depth_relief = min(
                0.04 if snapshot_stream_supportive else 0.03,
                float(execution_policy.get("snapshot_depth_bonus", 0.015) or 0.015)
                + max(0.0, depth_quality - minimum_usable_true_depth_quality) * 0.02
                + (0.005 if snapshot_stream_supportive else 0.0),
            )
        elif trusted_real_dom_fallback_support:
            snapshot_depth_relief = min(
                0.04,
                float(execution_policy.get("snapshot_depth_bonus", 0.015) or 0.015)
                + max(0.0, depth_quality - preferred_true_depth_min_quality) * 0.025
                + 0.005,
            )
        elif synthetic_depth_only:
            synthetic_depth_penalty = min(
                0.12,
                float(execution_policy.get("synthetic_depth_penalty", 0.04) or 0.04)
                + (0.02 if category_label == "crypto" and (continuation_family or continuation_entry) else 0.0),
            )
        elif misaligned_true_depth_available:
            misaligned_true_depth_penalty = min(
                0.12,
                misaligned_true_depth_penalty_scale
                + (0.02 if depth_quote_agreement_state == "severe_divergence" else 0.0),
            )
        elif low_trust_true_depth_available:
            low_trust_true_depth_penalty = min(0.10, low_trust_true_depth_penalty_scale)
        elif thin_true_depth_untrusted:
            thin_true_depth_penalty = min(0.12, thin_true_depth_penalty_scale)

        adaptive_policy_relief = asset_performance_relief + book_performance_relief + true_depth_relief + snapshot_depth_relief
        adaptive_policy_penalty = (
            asset_performance_penalty
            + book_performance_penalty
            + synthetic_depth_penalty
            + misaligned_true_depth_penalty
            + low_trust_true_depth_penalty
            + thin_true_depth_penalty
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
        depth_flow_stop_hunt_override = bool(
            depth_flow_sovereignty_rescue_candidate
            and stop_hunt_risk < 0.62
            and extension_score <= 1.28
            and directional_extension <= 0.88
            and target_efficiency_score >= 0.12
            and (
                strong_true_depth_support
                or directional_flow_support >= max(0.34, depth_sovereignty_min_directional_flow + 0.12)
                or external_confirmation_score >= 0.20
                or fast_entry_confirmation_ready
            )
        )
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
        if session_timing_strictness["risk_penalty"] > 0.0:
            risk_score += float(session_timing_strictness["risk_penalty"])
            reasons.append(str(session_timing_strictness["reason"]))

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
            if (signal.direction == "BUY" and structure_bias == "sell") or (
                signal.direction == "SELL" and structure_bias == "buy"
            ):
                risk_score += 0.28
                reasons.append("failed opposite reclaim now favors the other side")
        if continuation_family or continuation_entry:
            if trigger_reversal_against_trade:
                risk_score += 0.14
                reasons.append("trigger trend is now leaning against the continuation")
            if opposing_liquidity_sweep:
                risk_score += 0.12
                reasons.append("opposite-side sweep is undermining the continuation")
            if opposing_trigger_close and mature_continuation_profile:
                risk_score += 0.10
                reasons.append("trigger candle is closing against the continuation")
            if continuation_reclaim_pressure:
                risk_score += 0.18
                reasons.append("continuation is colliding with opposite-side reclaim pressure")
            if event_ladder_hostile_flow:
                risk_score += 0.12
                reasons.append("event-ladder flow is leaning against the continuation")
            if cross_asset_directional_conflict:
                risk_score += 0.06
                reasons.append("cross-asset spillover is leaning against the continuation")
            if event_ladder_cross_market_conflict:
                risk_score += 0.08 + (0.06 if strong_event_ladder_cross_market_conflict else 0.0)
                if event_ladder_cross_market_hard_block:
                    risk_score += 0.06
                reasons.append("event-ladder flow and cross-asset spillover are aligned against the continuation")
        if dom_ladder_ready and dom_stream_health_known:
            if dom_stream_hard_floor_breached:
                if trusted_real_dom_fallback_support:
                    risk_score += 0.06
                    reasons.append("event-ladder stream is degraded but trusted real DOM remains usable")
                else:
                    risk_score += 0.18
                    reasons.append("event-ladder stream health is too degraded to trust for timing")
                if (
                    not trusted_real_dom_fallback_support
                    and (continuation_family or continuation_entry)
                    and (
                        event_ladder_cross_market_conflict
                        or event_ladder_hostile_flow
                        or stop_hunt_risk >= 0.48
                    )
                ):
                    hard_blocks.append(
                        "event-ladder stream integrity has degraded while continuation pressure is already elevated"
                    )
            elif event_ladder_stream_health_blocks_sovereignty:
                risk_score += 0.10
                reasons.append("event-ladder stream health is degraded")
            elif dom_trade_stream_missing:
                risk_score += 0.04
                reasons.append("event-ladder trade flow is incomplete")
        if event_backed_true_depth_available and dom_fragmented_market and dom_fragmentation_score >= 0.42:
            risk_score += 0.08 if depth_fragmentation_untrusted else 0.04
            reasons.append("cross-venue depth is fragmented")
        if crypto_breadth_conflict:
            risk_score += 0.16 if is_crypto_alt else 0.10
            reasons.append("broad crypto breadth is leaning against the trade")
        if is_crypto and aligned_trade_flow <= -0.18:
            risk_score += 0.08 if is_crypto_alt else 0.05
            reasons.append("crypto trade flow is leaning against the trade")
        if crypto_derivative_conflict and not crypto_derivative_support:
            risk_score += 0.08 if is_crypto_alt else 0.05
            reasons.append("crypto derivatives are not backing the trade cleanly")
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
        if wants_retest and not impulse_fast_path_supported and not (breakout_retest_ready or first_pullback_ready):
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
        if continuation_rescue_candidate:
            risk_score -= 0.05
            if has_directional_flow_support:
                risk_score -= 0.04
            elif entry_confirmation_ready or breakout_retest_ready or first_pullback_ready:
                risk_score -= 0.02
        if high_conviction_continuation_candidate:
            risk_score -= 0.04
            if high_conviction_continuation_supported:
                if extension_score <= 1.05 and abs(vwap_distance_atr) <= 1.10:
                    risk_score -= 0.03
                if target_efficiency_score >= 0.22:
                    risk_score -= 0.03
                if impulse_age_bars <= 4:
                    risk_score -= 0.02
                if elite_pattern_rank >= 0.22:
                    risk_score -= 0.02
                if target_efficiency_score >= 0.14 and directional_extension <= 0.84:
                    risk_score -= 0.02
        if context_continuation_execution_candidate:
            risk_score -= 0.04
            if context_confirmation_override:
                risk_score -= 0.03
                if entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
                    risk_score -= 0.08
        if impulse_fast_path_candidate:
            risk_score -= 0.04
            if impulse_fast_path_supported:
                risk_score -= 0.05
                if extension_score <= 1.02 and abs(vwap_distance_atr) <= 1.10:
                    risk_score -= 0.03
                if target_efficiency_score >= 0.18:
                    risk_score -= 0.03
                if impulse_age_bars <= 3:
                    risk_score -= 0.02
        if depth_flow_sovereignty_candidate:
            risk_score -= 0.05
            if depth_sovereignty_source == "true_depth":
                risk_score -= 0.03
            if depth_flow_sovereignty_confirmation_override and entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
                risk_score -= 0.14
            if depth_flow_stop_hunt_override and stop_hunt_risk >= 0.48:
                risk_score -= 0.08
            if impulse_age_bars >= 6 and not failed_opposite_move_confirmed:
                risk_score -= 0.04
        elif depth_flow_sovereignty_rescue_candidate:
            risk_score -= 0.06
            if depth_sovereignty_source == "true_depth":
                risk_score -= 0.02
            if depth_flow_sovereignty_confirmation_override and entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
                risk_score -= 0.16
            if depth_flow_stop_hunt_override and stop_hunt_risk >= 0.48:
                risk_score -= 0.10
            if impulse_age_bars >= 6 and not failed_opposite_move_confirmed:
                risk_score -= 0.05
        if breakout_ignition_candidate:
            risk_score -= 0.05
            if strong_true_depth_support or trusted_real_dom_fallback_support:
                risk_score -= 0.03
            if breakout_ignition_confirmation_override and entry_confirmation_bars_required > 1 and not entry_confirmation_ready:
                risk_score -= 0.12
            if impulse_age_bars >= 6 and directional_extension <= 1.02:
                risk_score -= 0.04
        if inactivity_execution_relief:
            risk_score -= 0.04 + inactivity_relief_strength * 0.08
        if asset_performance_relief > 0.0:
            risk_score -= asset_performance_relief
        elif asset_performance_penalty > 0.0:
            risk_score += asset_performance_penalty
            reasons.append("recent asset-level execution has weakened")
        if book_performance_relief > 0.0:
            risk_score -= book_performance_relief
        elif book_performance_penalty > 0.0:
            risk_score += book_performance_penalty
            reasons.append("recent book-level execution has cooled off")
        if true_depth_relief > 0.0:
            risk_score -= true_depth_relief
            reasons.append("true depth confirms the late entry profile")
        elif synthetic_depth_penalty > 0.0:
            risk_score += synthetic_depth_penalty
            reasons.append("only synthetic depth is available for this continuation profile")
        elif misaligned_true_depth_penalty > 0.0:
            risk_score += misaligned_true_depth_penalty
            reasons.append("external book depth is diverging from the execution venue quote")
        elif low_trust_true_depth_penalty > 0.0:
            risk_score += low_trust_true_depth_penalty
            reasons.append("available true depth is not trusted enough to lean on for timing")
        elif thin_true_depth_penalty > 0.0:
            risk_score += thin_true_depth_penalty
            reasons.append("available book depth is too shallow to trust for execution timing")
        elif uninformative_true_depth_available:
            reasons.append("true depth is present but too flat to improve execution timing")

        if broker_agreement_state in {"divergent", "severe_divergence"} and broker_spread_regime in {"stressed", "extreme", "wide"}:
            hard_blocks.append("broker divergence and spread stress are both active")
        weak_candle_extension_limit = base_weak_candle_extension_limit + (0.07 if elite_supported_candidate else 0.0)
        weak_candle_floor = base_weak_candle_floor - (0.02 if elite_supported_candidate else 0.0)
        weak_candle_extension_limit += adaptive_policy_relief * 0.60 - adaptive_policy_penalty * 0.35
        weak_candle_floor = max(
            0.20,
            weak_candle_floor - adaptive_policy_relief * 0.18 + adaptive_policy_penalty * 0.12,
        )
        if continuation_rescue_candidate:
            weak_candle_extension_limit += 0.03
            weak_candle_floor = max(0.22, weak_candle_floor - 0.02)
        if high_conviction_continuation_supported:
            weak_candle_extension_limit += 0.04
            weak_candle_floor = max(0.22, weak_candle_floor - 0.01)
        if impulse_fast_path_supported:
            weak_candle_extension_limit += 0.06
            weak_candle_floor = max(0.20, weak_candle_floor - 0.03)
        if shock_fast_path_supported:
            weak_candle_extension_limit += 0.06
            weak_candle_floor = max(0.20, weak_candle_floor - 0.02)
        if depth_flow_sovereignty_rescue_candidate:
            weak_candle_extension_limit += 0.08 if depth_sovereignty_source == "true_depth" else 0.06
            weak_candle_floor = max(0.20, weak_candle_floor - (0.03 if depth_sovereignty_source == "true_depth" else 0.02))
        if breakout_ignition_candidate:
            weak_candle_extension_limit += 0.08 if depth_sovereignty_source == "true_depth" else 0.05
            weak_candle_floor = max(0.20, weak_candle_floor - (0.03 if depth_sovereignty_source == "true_depth" else 0.02))
        if inactivity_execution_relief:
            weak_candle_extension_limit += 0.02 + inactivity_relief_strength * 0.03
            weak_candle_floor = max(0.22, weak_candle_floor - (0.01 + inactivity_relief_strength * 0.02))
        weak_candle_extension_limit = max(
            0.50,
            weak_candle_extension_limit + float(session_timing_strictness["weak_candle_extension_delta"]),
        )
        weak_candle_floor = min(
            0.42,
            weak_candle_floor + float(session_timing_strictness["weak_candle_floor_delta"]),
        )
        if extension_score >= weak_candle_extension_limit and candle_quality_score <= weak_candle_floor:
            hard_blocks.append("entry is extended and the trigger candle is weak")
        target_efficiency_hard_floor = base_target_efficiency_hard_floor - (0.04 if elite_supported_candidate else 0.0)
        opposing_distance_hard_floor = base_opposing_distance_hard_floor - (
            0.0005 if elite_supported_candidate else 0.0
        )
        target_efficiency_hard_floor = max(
            0.08,
            target_efficiency_hard_floor - adaptive_policy_relief * 0.22 + adaptive_policy_penalty * 0.20,
        )
        opposing_distance_hard_floor = max(
            0.0020,
            opposing_distance_hard_floor - adaptive_policy_relief * 0.0020 + adaptive_policy_penalty * 0.0015,
        )
        if continuation_rescue_candidate:
            target_efficiency_hard_floor = max(0.09, target_efficiency_hard_floor - 0.02)
            opposing_distance_hard_floor = max(0.0026, opposing_distance_hard_floor - 0.0003)
        if high_conviction_continuation_supported:
            target_efficiency_hard_floor = max(0.10, target_efficiency_hard_floor - 0.03)
            opposing_distance_hard_floor = max(0.0028, opposing_distance_hard_floor - 0.0004)
        if impulse_fast_path_supported:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - 0.04)
            opposing_distance_hard_floor = max(0.0022, opposing_distance_hard_floor - 0.0006)
        if shock_fast_path_supported:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - 0.03)
            opposing_distance_hard_floor = max(0.0023, opposing_distance_hard_floor - 0.0005)
        if depth_flow_sovereignty_candidate:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - 0.02)
            opposing_distance_hard_floor = max(0.0024, opposing_distance_hard_floor - 0.0003)
        elif depth_flow_sovereignty_rescue_candidate:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - 0.03)
            opposing_distance_hard_floor = max(0.0022, opposing_distance_hard_floor - 0.0005)
        if breakout_ignition_candidate:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - 0.035)
            opposing_distance_hard_floor = max(0.0022, opposing_distance_hard_floor - 0.0005)
        if inactivity_execution_relief:
            target_efficiency_hard_floor = max(0.08, target_efficiency_hard_floor - (0.015 + inactivity_relief_strength * 0.03))
            opposing_distance_hard_floor = max(0.0024, opposing_distance_hard_floor - (0.0003 + inactivity_relief_strength * 0.0004))
        target_efficiency_hard_floor = min(
            0.32,
            target_efficiency_hard_floor + float(session_timing_strictness["target_efficiency_floor_delta"]),
        )
        opposing_distance_hard_floor = min(
            0.0100,
            opposing_distance_hard_floor + float(session_timing_strictness["opposing_distance_floor_delta"]),
        )
        if target_efficiency_score <= target_efficiency_hard_floor and opposing_distance <= opposing_distance_hard_floor:
            hard_blocks.append("too little clean space remains to the target")
        impulse_age_hard_limit = base_impulse_age_hard_limit + (1 if elite_supported_candidate else 0)
        directional_extension_hard_limit = base_directional_extension_hard_limit + (
            0.06 if elite_supported_candidate else 0.0
        )
        impulse_age_hard_limit = max(
            4,
            int(round(impulse_age_hard_limit + adaptive_policy_relief * 4.0 - adaptive_policy_penalty * 3.0)),
        )
        directional_extension_hard_limit = max(
            0.62,
            directional_extension_hard_limit + adaptive_policy_relief * 0.20 - adaptive_policy_penalty * 0.12,
        )
        if continuation_rescue_candidate:
            impulse_age_hard_limit += 1
            directional_extension_hard_limit += 0.04
        if high_conviction_continuation_supported:
            impulse_age_hard_limit += 1
            directional_extension_hard_limit += 0.05
        if impulse_fast_path_supported:
            impulse_age_hard_limit += 1
            directional_extension_hard_limit += 0.08
        if shock_fast_path_supported:
            impulse_age_hard_limit += 1
            directional_extension_hard_limit += 0.06
        if depth_flow_sovereignty_candidate:
            impulse_age_hard_limit += 1
            directional_extension_hard_limit += 0.05
        elif depth_flow_sovereignty_rescue_candidate:
            impulse_age_hard_limit += 2
            directional_extension_hard_limit += 0.08 if depth_sovereignty_source == "true_depth" else 0.06
        if breakout_ignition_candidate:
            impulse_age_hard_limit += 2
            directional_extension_hard_limit += 0.12 if depth_sovereignty_source == "true_depth" else 0.08
        if inactivity_execution_relief:
            impulse_age_hard_limit += 1 + (1 if inactivity_relief_strength >= 0.75 else 0)
            directional_extension_hard_limit += 0.03 + inactivity_relief_strength * 0.05
        impulse_age_hard_limit = max(
            3,
            int(impulse_age_hard_limit + int(session_timing_strictness["impulse_age_limit_delta"])),
        )
        directional_extension_hard_limit = max(
            0.56,
            directional_extension_hard_limit + float(session_timing_strictness["directional_extension_limit_delta"]),
        )
        if impulse_age_bars >= impulse_age_hard_limit and directional_extension >= directional_extension_hard_limit:
            hard_blocks.append("setup is too old and already stretched")
        if (
            high_conviction_continuation_candidate
            and not high_conviction_continuation_timing_intact
            and not has_directional_flow_support
        ):
            hard_blocks.append("high-conviction continuation has lost its timing edge")
        if wants_retest and not impulse_fast_path_supported and not shock_fast_path_supported and not breakout_retest_ready and not first_pullback_ready:
            hard_blocks.append("breakout entry has not earned a clean retest or first pullback")
        if directional_extension >= 0.82 and (
            exhaustion_risk >= 0.45 or dominant_exhaustion >= 0.60 or bias_exhausted
        ) and not (
            depth_flow_sovereignty_rescue_candidate
            and directional_extension <= 0.88
            and extension_score <= 1.28
        ):
            hard_blocks.append("extended entry is combining with exhaustion risk")
        if failed_opposite_move_confirmed and (
            (signal.direction == "BUY" and structure_bias == "sell")
            or (signal.direction == "SELL" and structure_bias == "buy")
        ):
            hard_blocks.append("failed opposite reclaim is confirmed against the trade direction")
        if crypto_flow_breadth_hard_block:
            hard_blocks.append("broad crypto breadth and live flow are aligned against the trade")
        if continuation_reclaim_hard_block:
            hard_blocks.append("continuation entry is fighting an opposite-side reclaim")
        if event_ladder_cross_market_hard_block:
            hard_blocks.append("event-ladder DOM, hostile flow, and cross-asset conflict are aligned against the continuation")
        if stop_hunt_risk >= 0.48 and (
            synthetic_depth_only
            or misaligned_true_depth_available
            or low_trust_true_depth_available
            or thin_true_depth_untrusted
            or uninformative_true_depth_available
        ) and not depth_flow_stop_hunt_override:
            hard_blocks.append("stop-hunt risk is elevated while usable execution depth is unavailable")
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
        if (
            entry_confirmation_bars_required > 1
            and not entry_confirmation_ready
            and not (impulse_fast_path_supported and fast_entry_confirmation_ready)
            and not (shock_fast_path_supported and (fast_entry_confirmation_ready or shock_confirmation_override))
            and not context_confirmation_override
            and not depth_flow_sovereignty_confirmation_override
            and not breakout_ignition_confirmation_override
        ):
            hard_blocks.append("entry confirmation delay is still pending")
        if (
            session_timing_strictness["require_confirmation"]
            and not entry_confirmation_ready
            and not (impulse_fast_path_supported and fast_entry_confirmation_ready)
            and not (shock_fast_path_supported and (fast_entry_confirmation_ready or shock_confirmation_override))
            and not context_confirmation_override
            and not depth_flow_sovereignty_confirmation_override
            and not breakout_ignition_confirmation_override
            and not breakout_retest_ready
            and not first_pullback_ready
            and not has_directional_flow_support
        ):
            hard_blocks.append("off-session entry has not earned cleaner confirmation")
        pattern_rank_hard_floor = (
            base_pattern_rank_strong_floor
            if strong_market_candidate and setup_quality >= 0.66 and alignment_score >= 0.74
            else base_pattern_rank_hard_floor
        )
        pattern_rank_hard_floor = max(
            0.02,
            pattern_rank_hard_floor - adaptive_policy_relief * 0.10 + adaptive_policy_penalty * 0.10,
        )
        if continuation_rescue_candidate:
            pattern_rank_hard_floor = max(0.02, pattern_rank_hard_floor - 0.04)
        if high_conviction_continuation_supported:
            pattern_rank_hard_floor = max(0.04, pattern_rank_hard_floor - 0.03)
        if impulse_fast_path_supported:
            pattern_rank_hard_floor = max(0.02, pattern_rank_hard_floor - 0.05)
        if shock_fast_path_supported:
            pattern_rank_hard_floor = max(0.02, pattern_rank_hard_floor - 0.04)
        if context_continuation_execution_candidate:
            pattern_rank_hard_floor = max(0.02, pattern_rank_hard_floor - 0.04)
        if depth_flow_sovereignty_candidate:
            pattern_rank_hard_floor = max(
                0.02,
                pattern_rank_hard_floor - (0.06 if depth_sovereignty_source == "true_depth" else 0.04),
            )
        elif depth_flow_sovereignty_rescue_candidate:
            pattern_rank_hard_floor = max(
                0.02,
                pattern_rank_hard_floor - (0.08 if depth_sovereignty_source == "true_depth" else 0.05),
            )
        if breakout_ignition_candidate:
            pattern_rank_hard_floor = max(
                0.02,
                pattern_rank_hard_floor - (0.07 if depth_sovereignty_source == "true_depth" else 0.05),
            )
        if inactivity_execution_relief:
            pattern_rank_hard_floor = max(0.04, pattern_rank_hard_floor - (0.015 + inactivity_relief_strength * 0.035))
        pattern_rank_hard_floor = min(
            0.28,
            pattern_rank_hard_floor + float(session_timing_strictness["pattern_rank_floor_delta"]),
        )
        low_pattern_rank_is_actionable = bool(
            blocked_recent_pattern
            or (
                recent_pattern_sample_count >= 8
                and not shock_fast_path_candidate
                and not context_continuation_execution_candidate
            )
            or (
                recent_pattern_sample_count >= 5
                and not has_directional_flow_support
                and not entry_confirmation_ready
                and not shock_fast_path_candidate
                and not context_continuation_execution_candidate
            )
            or not (
                continuation_rescue_candidate
                or high_conviction_continuation_candidate
                or impulse_fast_path_candidate
                or shock_fast_path_candidate
                or context_continuation_execution_candidate
                or depth_flow_sovereignty_candidate
                or depth_flow_sovereignty_rescue_candidate
                or breakout_ignition_candidate
            )
        )
        if (
            depth_flow_sovereignty_rescue_candidate
            and depth_flow_sovereignty_confirmation_override
            and recent_pattern_sample_count < 8
            and not blocked_recent_pattern
        ):
            low_pattern_rank_is_actionable = False
        if (
            breakout_ignition_candidate
            and breakout_ignition_confirmation_override
            and recent_pattern_sample_count < 8
            and not blocked_recent_pattern
        ):
            low_pattern_rank_is_actionable = False
        if (
            pattern_family != "unknown"
            and elite_pattern_rank <= pattern_rank_hard_floor
            and low_pattern_rank_is_actionable
        ):
            hard_blocks.append("pattern family ranks below elite threshold")
        cluster_hard_limit = 0.30 if elite_supported_candidate else 0.26
        if cluster_penalty >= cluster_hard_limit:
            hard_blocks.append("trade clustering risk is too high")
        if regime_entry_policy:
            if setup_quality < float(regime_entry_policy.get("min_setup_quality", 0.0) or 0.0) and candle_quality_score <= float(
                regime_entry_policy.get("min_candle_quality", 0.0) or 0.0
            ):
                hard_blocks.append("regime-specific entry policy rejects the setup")

        guarded_force_soft_blocks = {
            "entry confirmation delay is still pending",
            "pattern family ranks below elite threshold",
        }
        guarded_force_removed_blocks: List[str] = []
        guarded_force_blocked_by: List[str] = []
        guarded_force_condition_blocks: List[str] = []
        guarded_force_applied = False
        guarded_force_depth_ok = bool(
            depth_sovereignty_source == "true_depth"
            and strong_true_depth_support
            and directional_flow_support >= guarded_force_min_directional_flow
            and aligned_book_pressure >= guarded_force_min_book_pressure
            and directional_flow_conflict > -0.06
            and true_depth_directional_conflict > -0.04
            and not has_directional_flow_conflict
        )
        guarded_force_structure_ok = bool(
            alignment_score >= guarded_force_min_alignment
            and setup_quality >= guarded_force_min_setup_quality
            and candle_quality_score >= 0.26
            and session_quality_score >= 0.32
            and target_efficiency_score >= guarded_force_min_target_efficiency
            and opposing_distance > opposing_distance_hard_floor
            and extension_score <= guarded_force_max_extension_score
            and directional_extension <= guarded_force_max_directional_extension
            and impulse_age_bars <= impulse_age_hard_limit
            and directional_extension < directional_extension_hard_limit
            and stop_hunt_risk < guarded_force_max_stop_hunt_risk
            and not failed_opposite_move_confirmed
        )
        guarded_force_market_ok = bool(
            not depth_fragmentation_untrusted
            and not event_ladder_cross_market_conflict
            and not event_ladder_hostile_flow
            and not crypto_breadth_conflict
            and not (crypto_derivative_conflict and not crypto_derivative_support)
            and broker_quote_quality_state not in {"stale", "delayed"}
            and not (
                broker_agreement_state in {"divergent", "severe_divergence"}
                and broker_spread_regime in {"stressed", "extreme", "wide"}
            )
        )
        guarded_force_candidate = bool(
            guarded_force_entry_enabled
            and guarded_force_depth_ok
            and guarded_force_structure_ok
            and guarded_force_market_ok
        )
        guarded_force_safety_blocks = [
            block for block in hard_blocks if block not in guarded_force_soft_blocks
        ]
        if guarded_force_entry_enabled:
            if not guarded_force_depth_ok:
                guarded_force_condition_blocks.append("true-depth DOM support is insufficient")
            if not guarded_force_structure_ok:
                guarded_force_condition_blocks.append("entry structure is not forceable")
            if not guarded_force_market_ok:
                guarded_force_condition_blocks.append("market context is not forceable")
            guarded_force_blocked_by = list(
                dict.fromkeys(guarded_force_safety_blocks + guarded_force_condition_blocks)
            )
        if guarded_force_candidate and hard_blocks and not guarded_force_safety_blocks:
            guarded_force_removed_blocks = [
                block for block in hard_blocks if block in guarded_force_soft_blocks
            ]
            if guarded_force_removed_blocks:
                hard_blocks = [
                    block for block in hard_blocks if block not in guarded_force_soft_blocks
                ]
                risk_score = max(0.0, risk_score - guarded_force_risk_relief)
                notes.append("guarded_force_entry")
                reasons.append("guarded force used true-depth DOM to override soft execution gates")
                guarded_force_applied = True

        context_pressure_soft_override_removed_blocks: List[str] = []
        context_pressure_soft_override_blocked_by: List[str] = []
        context_pressure_soft_override_condition_blocks: List[str] = []
        context_pressure_soft_override_applied = False
        context_pressure_depth_kind = true_depth_provider_kind or "none"
        context_pressure_category_supported = category_label in {
            "crypto",
            "forex",
            "commodities",
            "commodity",
            "indices",
            "index",
        }
        if sidecar_true_depth_source:
            context_pressure_alignment_floor = 0.36 if category_label in {"commodities", "commodity"} else 0.40
            context_pressure_setup_floor = 0.48 if category_label in {"commodities", "commodity"} else 0.50
            context_pressure_context_floor = 0.15
            context_pressure_micro_floor = 0.58
            context_pressure_seed_floor = 0.66
            context_pressure_max_flow_conflict = -0.30
            context_pressure_target_floor = 0.08
            context_pressure_extension_ceiling = 1.08
        else:
            context_pressure_alignment_floor = 0.46 if category_label in {"crypto", "commodities", "commodity"} else 0.50
            context_pressure_setup_floor = (
                0.46
                if category_label == "crypto"
                else 0.48
                if category_label in {"commodities", "commodity"}
                else 0.50
            )
            context_pressure_context_floor = 0.14
            context_pressure_micro_floor = 0.64 if category_label == "crypto" else 0.60
            context_pressure_seed_floor = 0.68
            context_pressure_max_flow_conflict = -0.35 if category_label == "crypto" else -0.30
            context_pressure_target_floor = 0.08
            context_pressure_extension_ceiling = 1.20 if category_label == "crypto" else 1.12
        context_pressure_depth_ok = bool(
            context_pressure_category_supported
            and true_depth_available
            and preferred_true_depth
            and not synthetic_depth_only
            and (
                trusted_real_dom_book_available
                or trusted_snapshot_true_depth_available
                or trusted_real_dom_fallback_available
            )
            and true_depth_quote_aligned
            and depth_quote_alignment_score >= 0.80
            and not external_depth_rejected
            and not depth_fragmentation_untrusted
            and context_pressure_depth_kind in {"exchange", "sidecar", "redis"}
        )
        context_pressure_playbook_ok = bool(
            entry_style == "elite_context_pressure"
            and seed_score >= context_pressure_seed_floor
            and playbook_support_components >= 1
            and playbook_conflict_components == 0
            and playbook_micro_score >= context_pressure_micro_floor
            and (
                playbook_context_confluence >= context_pressure_context_floor
                or (
                    abs(playbook_cross_alignment) >= context_pressure_context_floor
                    and playbook_micro_score >= context_pressure_micro_floor + 0.08
                )
            )
        )
        context_pressure_structure_ok = bool(
            alignment_score >= context_pressure_alignment_floor
            and setup_quality >= context_pressure_setup_floor
            and candle_quality_score >= 0.24
            and session_quality_score >= 0.30
            and target_efficiency_score >= context_pressure_target_floor
            and opposing_distance > opposing_distance_hard_floor
            and extension_score <= context_pressure_extension_ceiling
            and directional_extension <= min(guarded_force_max_directional_extension + 0.08, 0.92)
            and impulse_age_bars <= impulse_age_hard_limit + 1
            and directional_extension < directional_extension_hard_limit + 0.10
            and stop_hunt_risk < min(guarded_force_max_stop_hunt_risk + 0.08, 0.56)
            and not failed_opposite_move_confirmed
        )
        context_pressure_market_ok = bool(
            guarded_force_market_ok
            and not crypto_flow_breadth_hard_block
            and not event_ladder_cross_market_hard_block
            and not (
                dom_stream_hard_floor_breached
                and not trusted_real_dom_fallback_available
            )
            and directional_flow_conflict > context_pressure_max_flow_conflict
            and true_depth_directional_conflict > max(context_pressure_max_flow_conflict, -0.28)
        )
        context_pressure_soft_override_candidate = bool(
            guarded_force_entry_enabled
            and context_pressure_depth_ok
            and context_pressure_playbook_ok
            and context_pressure_structure_ok
            and context_pressure_market_ok
        )
        context_pressure_soft_override_safety_blocks = [
            block for block in hard_blocks if block not in guarded_force_soft_blocks
        ]
        if guarded_force_entry_enabled:
            if not context_pressure_depth_ok:
                context_pressure_soft_override_condition_blocks.append(
                    "context-pressure depth is insufficient"
                )
            if not context_pressure_playbook_ok:
                context_pressure_soft_override_condition_blocks.append(
                    "context-pressure playbook proof is insufficient"
                )
            if not context_pressure_structure_ok:
                context_pressure_soft_override_condition_blocks.append(
                    "context-pressure structure is not forceable"
                )
            if not context_pressure_market_ok:
                context_pressure_soft_override_condition_blocks.append(
                    "context-pressure market context is not forceable"
                )
            context_pressure_soft_override_blocked_by = list(
                dict.fromkeys(
                    context_pressure_soft_override_safety_blocks
                    + context_pressure_soft_override_condition_blocks
                )
            )
        if (
            not guarded_force_applied
            and context_pressure_soft_override_candidate
            and hard_blocks
            and not context_pressure_soft_override_safety_blocks
        ):
            context_pressure_soft_override_removed_blocks = [
                block for block in hard_blocks if block in guarded_force_soft_blocks
            ]
            if context_pressure_soft_override_removed_blocks:
                hard_blocks = [
                    block for block in hard_blocks if block not in guarded_force_soft_blocks
                ]
                risk_score = max(0.0, risk_score - min(guarded_force_risk_relief, 0.14))
                notes.append("context_pressure_entry")
                reasons.append(
                    "elite context-pressure proof used trusted DOM to override soft execution gates"
                )
                context_pressure_soft_override_applied = True

        breakout_momentum_late_override_removed_blocks: List[str] = []
        breakout_momentum_late_override_blocked_by: List[str] = []
        breakout_momentum_late_override_condition_blocks: List[str] = []
        breakout_momentum_late_override_applied = False
        breakout_momentum_late_override_blocks = guarded_force_soft_blocks | {
            "setup is too old and already stretched",
        }
        breakout_momentum_direction_ok = bool(
            (
                signal.direction == "BUY"
                and structure_bias == "buy"
                and trend_5m == "trending_up"
            )
            or (
                signal.direction == "SELL"
                and structure_bias == "sell"
                and trend_5m == "trending_down"
            )
        )
        breakout_momentum_depth_ok = bool(
            true_depth_available
            and preferred_true_depth
            and true_depth_quote_aligned
            and not synthetic_depth_only
            and not external_depth_rejected
            and not depth_fragmentation_untrusted
            and not (
                dom_stream_hard_floor_breached
                and not trusted_real_dom_fallback_available
            )
        )
        breakout_momentum_playbook_ok = bool(
            category_label == "crypto"
            and entry_style in {"breakout_close", "breakout_ignition"}
            and seed_score >= 0.84
            and float(signal.confidence or 0.0) >= 0.66
            and playbook_support_components >= 1
            and playbook_conflict_components == 0
            and playbook_context_confluence >= 0.40
            and playbook_micro_score >= 0.80
            and abs(playbook_cross_alignment) >= 0.05
            and breakout_momentum_direction_ok
        )
        breakout_momentum_structure_ok = bool(
            alignment_score >= 0.46
            and setup_quality >= 0.52
            and candle_quality_score >= 0.24
            and session_quality_score >= 0.30
            and target_efficiency_score >= 0.08
            and opposing_distance > opposing_distance_hard_floor
            and extension_score <= 2.05
            and directional_extension <= 0.96
            and impulse_age_bars <= 9
            and cluster_penalty <= 0.20
            and stop_hunt_risk < 0.48
            and not failed_opposite_move_confirmed
        )
        breakout_momentum_market_ok = bool(
            guarded_force_market_ok
            and not crypto_flow_breadth_hard_block
            and not event_ladder_cross_market_hard_block
            and not event_ladder_hostile_flow
            and not continuation_reclaim_hard_block
            and directional_flow_conflict > -0.12
            and true_depth_directional_conflict > -0.08
        )
        breakout_momentum_late_override_candidate = bool(
            guarded_force_entry_enabled
            and breakout_momentum_depth_ok
            and breakout_momentum_playbook_ok
            and breakout_momentum_structure_ok
            and breakout_momentum_market_ok
        )
        breakout_momentum_late_override_safety_blocks = [
            block for block in hard_blocks if block not in breakout_momentum_late_override_blocks
        ]
        if guarded_force_entry_enabled:
            if not breakout_momentum_depth_ok:
                breakout_momentum_late_override_condition_blocks.append(
                    "breakout momentum depth is insufficient"
                )
            if not breakout_momentum_playbook_ok:
                breakout_momentum_late_override_condition_blocks.append(
                    "breakout momentum playbook proof is insufficient"
                )
            if not breakout_momentum_structure_ok:
                breakout_momentum_late_override_condition_blocks.append(
                    "breakout momentum structure is not forceable"
                )
            if not breakout_momentum_market_ok:
                breakout_momentum_late_override_condition_blocks.append(
                    "breakout momentum market context is not forceable"
                )
            breakout_momentum_late_override_blocked_by = list(
                dict.fromkeys(
                    breakout_momentum_late_override_safety_blocks
                    + breakout_momentum_late_override_condition_blocks
                )
            )
        if (
            not guarded_force_applied
            and not context_pressure_soft_override_applied
            and breakout_momentum_late_override_candidate
            and not breakout_momentum_late_override_safety_blocks
            and (hard_blocks or risk_score >= 0.50)
        ):
            breakout_momentum_late_override_removed_blocks = [
                block for block in hard_blocks if block in breakout_momentum_late_override_blocks
            ]
            if breakout_momentum_late_override_removed_blocks or risk_score >= 0.50:
                hard_blocks = [
                    block for block in hard_blocks if block not in breakout_momentum_late_override_blocks
                ]
                original_risk_score = risk_score
                risk_score = min(risk_score, 0.49)
                notes.append("breakout_momentum_late_entry")
                reasons.append(
                    "high-score breakout momentum proof overrode late-entry timing gates"
                )
                signal.metadata["breakout_momentum_late_override_original_risk_score"] = round(
                    original_risk_score,
                    4,
                )
                breakout_momentum_late_override_applied = True

        risk_kill_threshold = base_risk_kill_threshold + adaptive_policy_relief * 0.70 - adaptive_policy_penalty * 0.50
        if high_conviction_continuation_supported:
            risk_kill_threshold += 0.04
        if impulse_fast_path_supported:
            risk_kill_threshold += 0.03
        if shock_fast_path_supported:
            risk_kill_threshold += 0.03
        if depth_flow_sovereignty_candidate:
            risk_kill_threshold += 0.02
        elif depth_flow_sovereignty_rescue_candidate:
            risk_kill_threshold += 0.03
        if breakout_ignition_candidate:
            risk_kill_threshold += 0.03 if depth_sovereignty_source == "true_depth" else 0.02
        if inactivity_execution_relief:
            risk_kill_threshold += 0.02
        risk_kill_threshold = max(0.50, min(0.70, risk_kill_threshold))

        effective_execution_policy = {
            "asset": signal.asset,
            "category": category_label,
            "microstructure_source": microstructure_source,
            "depth_provider": depth_provider,
            "depth_provider_class": depth_provider_class,
            "true_depth_provider_kind": true_depth_provider_kind,
            "exchange_true_depth_source": exchange_true_depth_source,
            "sidecar_true_depth_source": sidecar_true_depth_source,
            "redis_true_depth_source": redis_true_depth_source,
            "depth_environment": depth_environment,
            "depth_quality": round(depth_quality, 4),
            "depth_quality_tier": depth_quality_tier,
            "depth_provider_trust_score": round(depth_provider_trust_score, 4),
            "depth_provider_trust_score_effective": round(depth_provider_trust_score_effective, 4),
            "depth_provider_trust_decay_applied": round(
                max(0.0, depth_provider_trust_score - depth_provider_trust_score_effective),
                4,
            ),
            "preferred_true_depth_min_trust_score": round(preferred_true_depth_min_trust_score, 4),
            "minimum_usable_true_depth_trust_score": round(minimum_usable_true_depth_trust_score, 4),
            "depth_quote_agreement_state": depth_quote_agreement_state,
            "depth_quote_alignment_score": round(depth_quote_alignment_score, 4),
            "external_depth_rejected": external_depth_rejected,
            "dom_stream_health_known": dom_stream_health_known,
            "dom_stream_health_score": round(dom_stream_health_score, 4),
            "dom_stream_trust_decay": round(dom_stream_trust_decay, 4),
            "dom_stream_degraded": dom_stream_degraded,
            "dom_depth_stream_missing": dom_depth_stream_missing,
            "dom_trade_stream_missing": dom_trade_stream_missing,
            "dom_stream_sovereignty_supported": dom_stream_sovereignty_supported,
            "dom_stream_hard_floor_breached": dom_stream_hard_floor_breached,
            "event_ladder_stream_health_blocks_sovereignty": event_ladder_stream_health_blocks_sovereignty,
            "depth_update_mode": depth_update_mode,
            "dom_event_backed": dom_event_backed,
            "dom_ladder_ready": dom_ladder_ready,
            "dom_stream_snapshot_ready": dom_stream_snapshot_ready,
            "dom_depth_event_age_seconds": signal.metadata.get("dom_depth_event_age_seconds"),
            "dom_snapshot_span_seconds": signal.metadata.get("dom_snapshot_span_seconds"),
            "dom_source_fidelity": dom_source_fidelity,
            "dom_liquidity_shift_proxy": round(dom_liquidity_shift_proxy, 4),
            "dom_sweep_pressure_proxy": round(dom_sweep_pressure_proxy, 4),
            "dom_refill_resilience_proxy": round(dom_refill_resilience_proxy, 4),
            "dom_absorption_proxy": round(dom_absorption_proxy, 4),
            "dom_iceberg_proxy": round(dom_iceberg_proxy, 4),
            "dom_queue_persistence": round(dom_queue_persistence, 4),
            "dom_add_intent_bias": round(dom_add_intent_bias, 4),
            "dom_cancel_pressure_bias": round(dom_cancel_pressure_bias, 4),
            "dom_queue_erosion_bias": round(dom_queue_erosion_bias, 4),
            "dom_trade_absorption_proxy": round(dom_trade_absorption_proxy, 4),
            "dom_refill_after_sweep_bias": round(dom_refill_after_sweep_bias, 4),
            "dom_trade_aggression_bias": round(dom_trade_aggression_bias, 4),
            "dom_trade_backed_iceberg_proxy": round(dom_trade_backed_iceberg_proxy, 4),
            "dom_fragmentation_score": round(dom_fragmentation_score, 4),
            "dom_fragmented_market": dom_fragmented_market,
            "snapshot_stream_supportive": snapshot_stream_supportive,
            "true_depth_available": true_depth_available,
            "usable_true_depth_available": usable_true_depth_available,
            "event_backed_true_depth_available": event_backed_true_depth_available,
            "snapshot_true_depth_available": snapshot_true_depth_available,
            "trusted_snapshot_true_depth_available": trusted_snapshot_true_depth_available,
            "trusted_real_dom_book_available": trusted_real_dom_book_available,
            "trusted_real_dom_fallback_available": trusted_real_dom_fallback_available,
            "trusted_real_dom_fallback_support": trusted_real_dom_fallback_support,
            "snapshot_true_depth_min_levels": int(snapshot_true_depth_min_levels),
            "guarded_force_entry_mode": guarded_force_entry_mode,
            "guarded_force_entry_enabled": guarded_force_entry_enabled,
            "guarded_force_min_directional_flow": round(guarded_force_min_directional_flow, 4),
            "guarded_force_min_book_pressure": round(guarded_force_min_book_pressure, 4),
            "guarded_force_min_alignment": round(guarded_force_min_alignment, 4),
            "guarded_force_min_setup_quality": round(guarded_force_min_setup_quality, 4),
            "guarded_force_min_target_efficiency": round(guarded_force_min_target_efficiency, 4),
            "guarded_force_max_extension_score": round(guarded_force_max_extension_score, 4),
            "guarded_force_max_directional_extension": round(guarded_force_max_directional_extension, 4),
            "guarded_force_max_stop_hunt_risk": round(guarded_force_max_stop_hunt_risk, 4),
            "guarded_force_risk_relief": round(guarded_force_risk_relief, 4),
            "guarded_force_candidate": guarded_force_candidate,
            "guarded_force_applied": guarded_force_applied,
            "guarded_force_removed_blocks": list(guarded_force_removed_blocks),
            "guarded_force_blocked_by": list(guarded_force_blocked_by),
            "context_pressure_depth_kind": context_pressure_depth_kind,
            "context_pressure_seed_floor": round(context_pressure_seed_floor, 4),
            "context_pressure_alignment_floor": round(context_pressure_alignment_floor, 4),
            "context_pressure_setup_floor": round(context_pressure_setup_floor, 4),
            "context_pressure_context_floor": round(context_pressure_context_floor, 4),
            "context_pressure_micro_floor": round(context_pressure_micro_floor, 4),
            "context_pressure_max_flow_conflict": round(context_pressure_max_flow_conflict, 4),
            "context_pressure_target_floor": round(context_pressure_target_floor, 4),
            "context_pressure_extension_ceiling": round(context_pressure_extension_ceiling, 4),
            "context_pressure_depth_ok": context_pressure_depth_ok,
            "context_pressure_playbook_ok": context_pressure_playbook_ok,
            "context_pressure_structure_ok": context_pressure_structure_ok,
            "context_pressure_market_ok": context_pressure_market_ok,
            "context_pressure_soft_override_candidate": context_pressure_soft_override_candidate,
            "context_pressure_soft_override_applied": context_pressure_soft_override_applied,
            "context_pressure_soft_override_removed_blocks": list(
                context_pressure_soft_override_removed_blocks
            ),
            "context_pressure_soft_override_blocked_by": list(
                context_pressure_soft_override_blocked_by
            ),
            "breakout_momentum_depth_ok": breakout_momentum_depth_ok,
            "breakout_momentum_playbook_ok": breakout_momentum_playbook_ok,
            "breakout_momentum_structure_ok": breakout_momentum_structure_ok,
            "breakout_momentum_market_ok": breakout_momentum_market_ok,
            "breakout_momentum_direction_ok": breakout_momentum_direction_ok,
            "breakout_momentum_late_override_candidate": breakout_momentum_late_override_candidate,
            "breakout_momentum_late_override_applied": breakout_momentum_late_override_applied,
            "breakout_momentum_late_override_removed_blocks": list(
                breakout_momentum_late_override_removed_blocks
            ),
            "breakout_momentum_late_override_blocked_by": list(
                breakout_momentum_late_override_blocked_by
            ),
            "preferred_true_depth": preferred_true_depth,
            "minimum_usable_true_depth_quality": round(minimum_usable_true_depth_quality, 4),
            "meets_true_depth_quality_floor": meets_true_depth_quality_floor,
            "meets_true_depth_trust_floor": meets_true_depth_trust_floor,
            "true_depth_quote_aligned": true_depth_quote_aligned,
            "misaligned_true_depth_available": misaligned_true_depth_available,
            "low_trust_true_depth_available": low_trust_true_depth_available,
            "thin_true_depth_untrusted": thin_true_depth_untrusted,
            "uninformative_true_depth_available": uninformative_true_depth_available,
            "true_depth_informative": true_depth_informative,
            "true_depth_signal_strength": round(true_depth_signal_strength, 4),
            "true_depth_directional_support": round(true_depth_directional_support, 4),
            "true_depth_directional_conflict": round(true_depth_directional_conflict, 4),
            "asset_score": round(asset_score, 4),
            "asset_action": asset_action,
            "book_score": round(book_score, 4),
            "book_action": book_action,
            "session_quality_label": session_quality_label,
            "session_timing_strictness": dict(session_timing_strictness),
            "policy_relief": round(adaptive_policy_relief, 4),
            "policy_penalty": round(adaptive_policy_penalty, 4),
            "true_depth_relief": round(true_depth_relief, 4),
            "snapshot_depth_relief": round(snapshot_depth_relief, 4),
            "trigger_reversal_against_trade": trigger_reversal_against_trade,
            "opposing_liquidity_sweep": opposing_liquidity_sweep,
            "opposing_trigger_close": opposing_trigger_close,
            "continuation_reclaim_evidence": int(continuation_reclaim_evidence),
            "mature_continuation_profile": mature_continuation_profile,
            "continuation_reclaim_pressure": continuation_reclaim_pressure,
            "cross_asset_directional_conflict": cross_asset_directional_conflict,
            "strong_cross_asset_directional_conflict": strong_cross_asset_directional_conflict,
            "cross_asset_primary_peer": cross_asset_primary_peer,
            "cross_asset_primary_relation": cross_asset_primary_relation,
            "cross_asset_supportive_direction": cross_asset_supportive_direction,
            "event_ladder_hostile_flow_component_count": int(event_ladder_hostile_flow_component_count),
            "event_ladder_hostile_flow": event_ladder_hostile_flow,
            "macro_spillover_conflict_relation": macro_spillover_conflict_relation,
            "event_ladder_cross_market_conflict": event_ladder_cross_market_conflict,
            "strong_event_ladder_cross_market_conflict": strong_event_ladder_cross_market_conflict,
            "event_ladder_cross_market_hard_block": event_ladder_cross_market_hard_block,
            "crypto_breadth_conflict": crypto_breadth_conflict,
            "crypto_breadth_support": crypto_breadth_support,
            "crypto_derivative_conflict": crypto_derivative_conflict,
            "crypto_derivative_support": crypto_derivative_support,
            "trade_flow_score": round(trade_flow_score, 4),
            "trade_delta_ratio": round(trade_delta_ratio, 4),
            "trade_cvd_slope": round(trade_cvd_slope, 4),
            "funding_bias": funding_bias,
            "oi_signal": oi_signal,
            "risk_kill_threshold": round(risk_kill_threshold, 4),
            "weak_candle_extension_limit": round(weak_candle_extension_limit, 4),
            "weak_candle_floor": round(weak_candle_floor, 4),
            "target_efficiency_hard_floor": round(target_efficiency_hard_floor, 4),
            "opposing_distance_hard_floor": round(opposing_distance_hard_floor, 6),
            "impulse_age_hard_limit": int(impulse_age_hard_limit),
            "directional_extension_hard_limit": round(directional_extension_hard_limit, 4),
            "pattern_rank_hard_floor": round(pattern_rank_hard_floor, 4),
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "fast_entry_confirmation_ready": bool(fast_entry_confirmation_ready),
            "fast_entry_confirmation_count": int(fast_entry_confirmation_count),
            "fast_entry_confirmation_bars_required": int(fast_entry_confirmation_bars_required),
            "external_confirmation_score": round(external_confirmation_score, 4),
            "shock_market_candidate": shock_market_candidate,
            "shock_score": round(shock_score, 4),
            "shock_event_score": round(shock_event_score, 4),
            "shock_displacement_score": round(shock_displacement_score, 4),
            "shock_structure_score": round(shock_structure_score, 4),
            "shock_liquidity_score": round(shock_liquidity_score, 4),
            "shock_timing_score": round(shock_timing_score, 4),
            "headline_shock_score": round(headline_shock_score, 4),
            "shock_fresh_event": bool(shock_fresh_event),
            "shock_supported": bool(shock_supported),
            "impulse_fast_path_candidate": impulse_fast_path_candidate,
            "impulse_fast_path_timing_intact": impulse_fast_path_timing_intact,
            "impulse_fast_path_supported": impulse_fast_path_supported,
            "shock_fast_path_candidate": shock_fast_path_candidate,
            "shock_fast_path_timing_intact": shock_fast_path_timing_intact,
            "shock_fast_path_supported": shock_fast_path_supported,
            "shock_confirmation_override": shock_confirmation_override,
            "breakout_ignition_candidate": breakout_ignition_candidate,
            "breakout_ignition_confirmation_override": breakout_ignition_confirmation_override,
            "context_continuation_execution_candidate": context_continuation_execution_candidate,
            "context_confirmation_override": context_confirmation_override,
            "seed_score": round(seed_score, 4),
            "playbook_context_confluence": round(playbook_context_confluence, 4),
            "playbook_cross_alignment": round(playbook_cross_alignment, 4),
            "playbook_micro_score": round(playbook_micro_score, 4),
            "playbook_support_components": int(playbook_support_components),
            "playbook_conflict_components": int(playbook_conflict_components),
            "depth_sovereignty_supported": depth_sovereignty_supported,
            "depth_sovereignty_source": depth_sovereignty_source,
            "depth_sovereignty_reason": depth_sovereignty_reason,
            "strong_true_depth_support": strong_true_depth_support,
            "strong_flow_support": strong_flow_support,
            "snapshot_dom_requires_confirmation": snapshot_dom_requires_confirmation,
            "dom_stream_snapshot_ready": dom_stream_snapshot_ready,
            "snapshot_stream_supportive": snapshot_stream_supportive,
            "dom_liquidity_shift_proxy": round(dom_liquidity_shift_proxy, 4),
            "dom_sweep_pressure_proxy": round(dom_sweep_pressure_proxy, 4),
            "dom_refill_resilience_proxy": round(dom_refill_resilience_proxy, 4),
            "dom_absorption_proxy": round(dom_absorption_proxy, 4),
            "dom_iceberg_proxy": round(dom_iceberg_proxy, 4),
            "dom_queue_persistence": round(dom_queue_persistence, 4),
            "depth_flow_sovereignty_candidate": depth_flow_sovereignty_candidate,
            "depth_flow_sovereignty_rescue_candidate": depth_flow_sovereignty_rescue_candidate,
            "depth_flow_sovereignty_confirmation_override": depth_flow_sovereignty_confirmation_override,
            "depth_flow_stop_hunt_override": depth_flow_stop_hunt_override,
        }

        signal.metadata["late_entry_risk_score"] = round(risk_score, 4)
        signal.metadata["late_entry_risk_reasons"] = list(reasons)
        signal.metadata["execution_hard_blocks"] = list(hard_blocks)
        signal.metadata["guarded_force_entry_mode"] = guarded_force_entry_mode
        signal.metadata["guarded_force_candidate"] = guarded_force_candidate
        signal.metadata["guarded_force_applied"] = guarded_force_applied
        signal.metadata["guarded_force_removed_blocks"] = list(guarded_force_removed_blocks)
        signal.metadata["guarded_force_blocked_by"] = list(guarded_force_blocked_by)
        signal.metadata["true_depth_provider_kind"] = true_depth_provider_kind
        signal.metadata["exchange_true_depth_source"] = exchange_true_depth_source
        signal.metadata["sidecar_true_depth_source"] = sidecar_true_depth_source
        signal.metadata["redis_true_depth_source"] = redis_true_depth_source
        signal.metadata["seed_score"] = round(seed_score, 4)
        signal.metadata["playbook_context_confluence"] = round(playbook_context_confluence, 4)
        signal.metadata["playbook_cross_alignment"] = round(playbook_cross_alignment, 4)
        signal.metadata["playbook_micro_score"] = round(playbook_micro_score, 4)
        signal.metadata["playbook_support_components"] = int(playbook_support_components)
        signal.metadata["playbook_conflict_components"] = int(playbook_conflict_components)
        signal.metadata["context_pressure_depth_kind"] = context_pressure_depth_kind
        signal.metadata["context_pressure_soft_override_candidate"] = (
            context_pressure_soft_override_candidate
        )
        signal.metadata["context_pressure_soft_override_applied"] = (
            context_pressure_soft_override_applied
        )
        signal.metadata["context_pressure_soft_override_removed_blocks"] = list(
            context_pressure_soft_override_removed_blocks
        )
        signal.metadata["context_pressure_soft_override_blocked_by"] = list(
            context_pressure_soft_override_blocked_by
        )
        signal.metadata["breakout_momentum_late_override_candidate"] = (
            breakout_momentum_late_override_candidate
        )
        signal.metadata["breakout_momentum_late_override_applied"] = (
            breakout_momentum_late_override_applied
        )
        signal.metadata["breakout_momentum_late_override_removed_blocks"] = list(
            breakout_momentum_late_override_removed_blocks
        )
        signal.metadata["breakout_momentum_late_override_blocked_by"] = list(
            breakout_momentum_late_override_blocked_by
        )
        signal.metadata["depth_provider_trust_score_effective"] = round(
            depth_provider_trust_score_effective,
            4,
        )
        signal.metadata["depth_provider_trust_decay_applied"] = round(
            max(0.0, depth_provider_trust_score - depth_provider_trust_score_effective),
            4,
        )
        signal.metadata["event_ladder_hostile_flow"] = event_ladder_hostile_flow
        signal.metadata["event_ladder_hostile_flow_component_count"] = int(
            event_ladder_hostile_flow_component_count
        )
        signal.metadata["event_ladder_cross_market_conflict"] = event_ladder_cross_market_conflict
        signal.metadata["event_ladder_cross_market_hard_block"] = event_ladder_cross_market_hard_block
        signal.metadata["cross_asset_directional_conflict"] = cross_asset_directional_conflict
        signal.metadata["dom_stream_health_blocks_sovereignty"] = (
            event_ladder_stream_health_blocks_sovereignty
        )
        signal.metadata["dom_stream_health_hard_floor_breached"] = dom_stream_hard_floor_breached
        signal.metadata["effective_execution_policy"] = dict(effective_execution_policy)
        signal.metadata["execution_relief_flags"] = {
            "strong_market_candidate": strong_market_candidate,
            "shock_market_candidate": shock_market_candidate,
            "strong_fx_crypto_candidate": strong_fx_crypto_candidate,
            "elite_supported_candidate": elite_supported_candidate,
            "continuation_rescue_candidate": continuation_rescue_candidate,
            "high_conviction_continuation_candidate": high_conviction_continuation_candidate,
            "high_conviction_continuation_timing_intact": high_conviction_continuation_timing_intact,
            "high_conviction_continuation_supported": high_conviction_continuation_supported,
            "impulse_fast_path_candidate": impulse_fast_path_candidate,
            "impulse_fast_path_timing_intact": impulse_fast_path_timing_intact,
            "impulse_fast_path_supported": impulse_fast_path_supported,
            "shock_score": round(shock_score, 4),
            "shock_event_score": round(shock_event_score, 4),
            "shock_displacement_score": round(shock_displacement_score, 4),
            "shock_structure_score": round(shock_structure_score, 4),
            "shock_liquidity_score": round(shock_liquidity_score, 4),
            "shock_timing_score": round(shock_timing_score, 4),
            "headline_shock_score": round(headline_shock_score, 4),
            "shock_fresh_event": shock_fresh_event,
            "shock_supported": shock_supported,
            "shock_fast_path_candidate": shock_fast_path_candidate,
            "shock_fast_path_timing_intact": shock_fast_path_timing_intact,
            "shock_fast_path_supported": shock_fast_path_supported,
            "shock_confirmation_override": shock_confirmation_override,
            "breakout_ignition_candidate": breakout_ignition_candidate,
            "breakout_ignition_confirmation_override": breakout_ignition_confirmation_override,
            "context_continuation_execution_candidate": context_continuation_execution_candidate,
            "context_confirmation_override": context_confirmation_override,
            "seed_score": round(seed_score, 4),
            "playbook_context_confluence": round(playbook_context_confluence, 4),
            "playbook_cross_alignment": round(playbook_cross_alignment, 4),
            "playbook_micro_score": round(playbook_micro_score, 4),
            "playbook_support_components": int(playbook_support_components),
            "playbook_conflict_components": int(playbook_conflict_components),
            "has_directional_flow_support": has_directional_flow_support,
            "has_directional_flow_conflict": has_directional_flow_conflict,
            "depth_sovereignty_supported": depth_sovereignty_supported,
            "depth_sovereignty_source": depth_sovereignty_source,
            "depth_sovereignty_reason": depth_sovereignty_reason,
            "strong_true_depth_support": strong_true_depth_support,
            "strong_flow_support": strong_flow_support,
            "trusted_snapshot_true_depth_available": trusted_snapshot_true_depth_available,
            "trusted_real_dom_book_available": trusted_real_dom_book_available,
            "trusted_real_dom_fallback_available": trusted_real_dom_fallback_available,
            "trusted_real_dom_fallback_support": trusted_real_dom_fallback_support,
            "guarded_force_entry_mode": guarded_force_entry_mode,
            "guarded_force_entry_enabled": guarded_force_entry_enabled,
            "guarded_force_depth_ok": guarded_force_depth_ok,
            "guarded_force_structure_ok": guarded_force_structure_ok,
            "guarded_force_market_ok": guarded_force_market_ok,
            "guarded_force_candidate": guarded_force_candidate,
            "guarded_force_applied": guarded_force_applied,
            "guarded_force_removed_blocks": list(guarded_force_removed_blocks),
            "guarded_force_blocked_by": list(guarded_force_blocked_by),
            "true_depth_provider_kind": true_depth_provider_kind,
            "exchange_true_depth_source": exchange_true_depth_source,
            "sidecar_true_depth_source": sidecar_true_depth_source,
            "redis_true_depth_source": redis_true_depth_source,
            "context_pressure_depth_kind": context_pressure_depth_kind,
            "context_pressure_depth_ok": context_pressure_depth_ok,
            "context_pressure_playbook_ok": context_pressure_playbook_ok,
            "context_pressure_structure_ok": context_pressure_structure_ok,
            "context_pressure_market_ok": context_pressure_market_ok,
            "context_pressure_soft_override_candidate": context_pressure_soft_override_candidate,
            "context_pressure_soft_override_applied": context_pressure_soft_override_applied,
            "context_pressure_soft_override_removed_blocks": list(
                context_pressure_soft_override_removed_blocks
            ),
            "context_pressure_soft_override_blocked_by": list(
                context_pressure_soft_override_blocked_by
            ),
            "breakout_momentum_depth_ok": breakout_momentum_depth_ok,
            "breakout_momentum_playbook_ok": breakout_momentum_playbook_ok,
            "breakout_momentum_structure_ok": breakout_momentum_structure_ok,
            "breakout_momentum_market_ok": breakout_momentum_market_ok,
            "breakout_momentum_direction_ok": breakout_momentum_direction_ok,
            "breakout_momentum_late_override_candidate": breakout_momentum_late_override_candidate,
            "breakout_momentum_late_override_applied": breakout_momentum_late_override_applied,
            "breakout_momentum_late_override_removed_blocks": list(
                breakout_momentum_late_override_removed_blocks
            ),
            "breakout_momentum_late_override_blocked_by": list(
                breakout_momentum_late_override_blocked_by
            ),
            "dom_stream_health_known": dom_stream_health_known,
            "dom_stream_health_score": round(dom_stream_health_score, 4),
            "dom_stream_trust_decay": round(dom_stream_trust_decay, 4),
            "dom_stream_degraded": dom_stream_degraded,
            "dom_depth_stream_missing": dom_depth_stream_missing,
            "dom_trade_stream_missing": dom_trade_stream_missing,
            "dom_stream_sovereignty_supported": dom_stream_sovereignty_supported,
            "dom_stream_hard_floor_breached": dom_stream_hard_floor_breached,
            "event_ladder_stream_health_blocks_sovereignty": event_ladder_stream_health_blocks_sovereignty,
            "snapshot_dom_requires_confirmation": snapshot_dom_requires_confirmation,
            "snapshot_stream_supportive": snapshot_stream_supportive,
            "snapshot_depth_relief": round(snapshot_depth_relief, 4),
            "dom_add_intent_bias": round(dom_add_intent_bias, 4),
            "dom_cancel_pressure_bias": round(dom_cancel_pressure_bias, 4),
            "dom_queue_erosion_bias": round(dom_queue_erosion_bias, 4),
            "dom_trade_absorption_proxy": round(dom_trade_absorption_proxy, 4),
            "dom_refill_after_sweep_bias": round(dom_refill_after_sweep_bias, 4),
            "dom_trade_aggression_bias": round(dom_trade_aggression_bias, 4),
            "dom_trade_backed_iceberg_proxy": round(dom_trade_backed_iceberg_proxy, 4),
            "dom_fragmentation_score": round(dom_fragmentation_score, 4),
            "dom_fragmented_market": dom_fragmented_market,
            "depth_fragmentation_untrusted": depth_fragmentation_untrusted,
            "depth_flow_sovereignty_candidate": depth_flow_sovereignty_candidate,
            "depth_flow_sovereignty_rescue_candidate": depth_flow_sovereignty_rescue_candidate,
            "depth_flow_sovereignty_confirmation_override": depth_flow_sovereignty_confirmation_override,
            "depth_flow_stop_hunt_override": depth_flow_stop_hunt_override,
            "continuation_reclaim_pressure": continuation_reclaim_pressure,
            "cross_asset_directional_conflict": cross_asset_directional_conflict,
            "strong_cross_asset_directional_conflict": strong_cross_asset_directional_conflict,
            "event_ladder_hostile_flow_component_count": int(event_ladder_hostile_flow_component_count),
            "event_ladder_hostile_flow": event_ladder_hostile_flow,
            "macro_spillover_conflict_relation": macro_spillover_conflict_relation,
            "event_ladder_cross_market_conflict": event_ladder_cross_market_conflict,
            "strong_event_ladder_cross_market_conflict": strong_event_ladder_cross_market_conflict,
            "event_ladder_cross_market_hard_block": event_ladder_cross_market_hard_block,
            "crypto_breadth_conflict": crypto_breadth_conflict,
            "crypto_breadth_support": crypto_breadth_support,
            "crypto_derivative_conflict": crypto_derivative_conflict,
            "crypto_derivative_support": crypto_derivative_support,
            "trade_flow_support": aligned_trade_flow >= 0.12,
            "trade_flow_conflict": aligned_trade_flow <= -0.16,
            "inactivity_execution_relief": inactivity_execution_relief,
            "inactivity_relief_strength": round(inactivity_relief_strength, 4),
            "inactivity_flat_book": inactivity_flat_book,
        }
        data["late_entry_risk"] = {
            "score": round(risk_score, 4),
            "playbook_name": playbook_name,
            "entry_style": entry_style,
            "seed_score": round(seed_score, 4),
            "playbook_context_confluence": round(playbook_context_confluence, 4),
            "playbook_cross_alignment": round(playbook_cross_alignment, 4),
            "playbook_micro_score": round(playbook_micro_score, 4),
            "playbook_support_components": int(playbook_support_components),
            "playbook_conflict_components": int(playbook_conflict_components),
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
            "recent_pattern_sample_count": recent_pattern_sample_count,
            "pattern_rank_score": round(elite_pattern_rank, 4),
            "cross_asset_alignment": round(cross_asset_alignment, 4),
            "cross_asset_confidence": round(cross_asset_confidence, 4),
            "cross_asset_primary_peer": cross_asset_primary_peer,
            "cross_asset_primary_relation": cross_asset_primary_relation,
            "cross_asset_supportive_direction": cross_asset_supportive_direction,
            "cross_asset_directional_conflict": cross_asset_directional_conflict,
            "strong_cross_asset_directional_conflict": strong_cross_asset_directional_conflict,
            "canonical_asset": canonical_asset,
            "funding_bias": funding_bias,
            "oi_signal": oi_signal,
            "trade_flow_score": round(trade_flow_score, 4),
            "trade_delta_ratio": round(trade_delta_ratio, 4),
            "trade_cvd_slope": round(trade_cvd_slope, 4),
            "trade_cluster_penalty": round(cluster_penalty, 4),
            "depth_provider_trust_score": round(depth_provider_trust_score, 4),
            "depth_provider_trust_score_effective": round(depth_provider_trust_score_effective, 4),
            "true_depth_provider_kind": true_depth_provider_kind,
            "exchange_true_depth_source": exchange_true_depth_source,
            "sidecar_true_depth_source": sidecar_true_depth_source,
            "redis_true_depth_source": redis_true_depth_source,
            "depth_provider_trust_decay_applied": round(
                max(0.0, depth_provider_trust_score - depth_provider_trust_score_effective),
                4,
            ),
            "dom_stream_health_known": dom_stream_health_known,
            "dom_stream_health_score": round(dom_stream_health_score, 4),
            "dom_stream_trust_decay": round(dom_stream_trust_decay, 4),
            "dom_stream_degraded": dom_stream_degraded,
            "dom_depth_stream_missing": dom_depth_stream_missing,
            "dom_trade_stream_missing": dom_trade_stream_missing,
            "dom_stream_sovereignty_supported": dom_stream_sovereignty_supported,
            "dom_stream_hard_floor_breached": dom_stream_hard_floor_breached,
            "event_ladder_stream_health_blocks_sovereignty": event_ladder_stream_health_blocks_sovereignty,
            "pattern_family": pattern_family,
            "failed_opposite_move_confirmed": failed_opposite_move_confirmed,
            "entry_confirmation_bars_required": int(entry_confirmation_bars_required),
            "entry_confirmation_count": int(entry_confirmation_count),
            "entry_confirmation_ready": entry_confirmation_ready,
            "fast_entry_confirmation_bars_required": int(fast_entry_confirmation_bars_required),
            "fast_entry_confirmation_count": int(fast_entry_confirmation_count),
            "fast_entry_confirmation_ready": fast_entry_confirmation_ready,
            "external_confirmation_score": round(external_confirmation_score, 4),
            "regime_entry_policy": regime_entry_policy,
            "vwap_distance_atr": round(vwap_distance_atr, 4),
            "session_quality_label": session_quality_label,
            "session_quality_score": round(session_quality_score, 4),
            "candle_quality_score": round(candle_quality_score, 4),
            "extension_score": round(extension_score, 4),
            "target_efficiency_score": round(target_efficiency_score, 4),
            "impulse_age_bars": int(impulse_age_bars),
            "breakout_retest_ready": breakout_retest_ready,
            "first_pullback_ready": first_pullback_ready,
            "liquidity_sweep_buy": liquidity_sweep_buy,
            "liquidity_sweep_sell": liquidity_sweep_sell,
            "trend_5m": trend_5m,
            "close_location": round(close_location, 4),
            "trigger_reversal_against_trade": trigger_reversal_against_trade,
            "opposing_liquidity_sweep": opposing_liquidity_sweep,
            "opposing_trigger_close": opposing_trigger_close,
            "continuation_reclaim_evidence": int(continuation_reclaim_evidence),
            "continuation_reclaim_pressure": continuation_reclaim_pressure,
            "event_ladder_hostile_flow_component_count": int(event_ladder_hostile_flow_component_count),
            "event_ladder_hostile_flow": event_ladder_hostile_flow,
            "macro_spillover_conflict_relation": macro_spillover_conflict_relation,
            "event_ladder_cross_market_conflict": event_ladder_cross_market_conflict,
            "strong_event_ladder_cross_market_conflict": strong_event_ladder_cross_market_conflict,
            "event_ladder_cross_market_hard_block": event_ladder_cross_market_hard_block,
            "alignment_score": round(alignment_score, 4),
            "strong_market_candidate": strong_market_candidate,
            "strong_fx_crypto_candidate": strong_fx_crypto_candidate,
            "elite_supported_candidate": elite_supported_candidate,
            "continuation_rescue_candidate": continuation_rescue_candidate,
            "high_conviction_continuation_candidate": high_conviction_continuation_candidate,
            "high_conviction_continuation_timing_intact": high_conviction_continuation_timing_intact,
            "high_conviction_continuation_supported": high_conviction_continuation_supported,
            "impulse_fast_path_candidate": impulse_fast_path_candidate,
            "impulse_fast_path_timing_intact": impulse_fast_path_timing_intact,
            "impulse_fast_path_supported": impulse_fast_path_supported,
            "crypto_breadth_conflict": crypto_breadth_conflict,
            "crypto_breadth_support": crypto_breadth_support,
            "crypto_derivative_conflict": crypto_derivative_conflict,
            "crypto_derivative_support": crypto_derivative_support,
            "directional_flow_support": round(directional_flow_support, 4),
            "directional_flow_conflict": round(directional_flow_conflict, 4),
            "depth_sovereignty_supported": depth_sovereignty_supported,
            "depth_sovereignty_source": depth_sovereignty_source,
            "depth_sovereignty_reason": depth_sovereignty_reason,
            "strong_true_depth_support": strong_true_depth_support,
            "strong_flow_support": strong_flow_support,
            "trusted_snapshot_true_depth_available": trusted_snapshot_true_depth_available,
            "guarded_force_entry_mode": guarded_force_entry_mode,
            "guarded_force_depth_ok": guarded_force_depth_ok,
            "guarded_force_structure_ok": guarded_force_structure_ok,
            "guarded_force_market_ok": guarded_force_market_ok,
            "guarded_force_candidate": guarded_force_candidate,
            "guarded_force_applied": guarded_force_applied,
            "guarded_force_removed_blocks": list(guarded_force_removed_blocks),
            "guarded_force_blocked_by": list(guarded_force_blocked_by),
            "context_pressure_depth_kind": context_pressure_depth_kind,
            "context_pressure_depth_ok": context_pressure_depth_ok,
            "context_pressure_playbook_ok": context_pressure_playbook_ok,
            "context_pressure_structure_ok": context_pressure_structure_ok,
            "context_pressure_market_ok": context_pressure_market_ok,
            "context_pressure_soft_override_candidate": context_pressure_soft_override_candidate,
            "context_pressure_soft_override_applied": context_pressure_soft_override_applied,
            "context_pressure_soft_override_removed_blocks": list(
                context_pressure_soft_override_removed_blocks
            ),
            "context_pressure_soft_override_blocked_by": list(
                context_pressure_soft_override_blocked_by
            ),
            "snapshot_dom_requires_confirmation": snapshot_dom_requires_confirmation,
            "dom_stream_snapshot_ready": dom_stream_snapshot_ready,
            "snapshot_stream_supportive": snapshot_stream_supportive,
            "dom_liquidity_shift_proxy": round(dom_liquidity_shift_proxy, 4),
            "dom_sweep_pressure_proxy": round(dom_sweep_pressure_proxy, 4),
            "dom_refill_resilience_proxy": round(dom_refill_resilience_proxy, 4),
            "dom_absorption_proxy": round(dom_absorption_proxy, 4),
            "dom_iceberg_proxy": round(dom_iceberg_proxy, 4),
            "dom_queue_persistence": round(dom_queue_persistence, 4),
            "dom_add_intent_bias": round(dom_add_intent_bias, 4),
            "dom_cancel_pressure_bias": round(dom_cancel_pressure_bias, 4),
            "dom_queue_erosion_bias": round(dom_queue_erosion_bias, 4),
            "dom_trade_absorption_proxy": round(dom_trade_absorption_proxy, 4),
            "dom_refill_after_sweep_bias": round(dom_refill_after_sweep_bias, 4),
            "dom_trade_aggression_bias": round(dom_trade_aggression_bias, 4),
            "dom_trade_backed_iceberg_proxy": round(dom_trade_backed_iceberg_proxy, 4),
            "dom_fragmentation_score": round(dom_fragmentation_score, 4),
            "dom_fragmented_market": dom_fragmented_market,
            "depth_flow_sovereignty_candidate": depth_flow_sovereignty_candidate,
            "depth_flow_sovereignty_rescue_candidate": depth_flow_sovereignty_rescue_candidate,
            "depth_flow_sovereignty_confirmation_override": depth_flow_sovereignty_confirmation_override,
            "depth_flow_stop_hunt_override": depth_flow_stop_hunt_override,
            "asset_performance_profile": dict(asset_performance),
            "book_performance_profile": dict(book_performance),
            "session_timing_strictness": dict(session_timing_strictness),
            "effective_execution_policy": dict(effective_execution_policy),
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

        if risk_score >= risk_kill_threshold:
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

        if risk_score >= max(0.52, risk_kill_threshold - 0.06):
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
    def _execution_source_floor_gate(
        signal: Signal,
        context: Dict[str, Any],
        conf_before: float,
        data: Dict[str, Any],
    ) -> bool:
        scorecard = signal.metadata.get("scorecard")
        if not isinstance(scorecard, dict):
            return True
        breakdown = scorecard.get("breakdown")
        if not isinstance(breakdown, dict) or not breakdown:
            return True

        category = str(signal.category or context.get("category") or "").strip().lower()
        valid_families = set(str(item) for item in (signal.metadata.get("valid_source_families") or []))
        checks: List[Tuple[str, float, float, bool]] = []

        def _score(name: str) -> Optional[float]:
            value = breakdown.get(name)
            if value is None:
                return None
            try:
                return float(value)
            except Exception:
                return None

        for name, floor in (("structure", 0.26), ("regime", 0.24), ("entry", 0.24)):
            score = _score(name)
            if score is not None:
                checks.append((name, score, floor, True))

        flow_score = max(
            float(_score("microstructure") or 0.0),
            float(_score("order_flow") or 0.0),
        )
        if ("flow" in valid_families or category == "crypto") and flow_score > 0.0:
            checks.append(("flow", flow_score, 0.22 if category == "crypto" else 0.18, category == "crypto"))

        if "cross_asset" in valid_families:
            cross_score = _score("cross_asset")
            if cross_score is not None:
                checks.append(("cross_asset", cross_score, 0.18, False))

        if "sentiment" in valid_families and category in {"crypto", "indices", "commodities", "forex"}:
            sentiment_score = _score("sentiment")
            if sentiment_score is not None:
                checks.append(("sentiment", sentiment_score, 0.16 if category != "forex" else 0.14, False))

        misses = [
            {
                "name": name,
                "score": round(score, 4),
                "floor": round(floor, 4),
                "critical": critical,
            }
            for name, score, floor, critical in checks
            if score < floor
        ]
        severe_misses = [item for item in misses if item["critical"] and item["score"] <= item["floor"] - 0.10]

        guard = {
            "checks": [
                {
                    "name": name,
                    "score": round(score, 4),
                    "floor": round(floor, 4),
                    "critical": critical,
                }
                for name, score, floor, critical in checks
            ],
            "misses": list(misses),
            "action": "pass",
        }
        signal.metadata["source_floor_guard"] = dict(guard)
        data["source_floor_guard"] = dict(guard)

        if not misses:
            return True

        if severe_misses or len(misses) >= 2:
            reason = "source floor failure: " + ", ".join(
                f"{item['name']} {item['score']:.2f}<{item['floor']:.2f}" for item in misses
            )
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
            guard["action"] = "block"
            signal.metadata["source_floor_guard"] = dict(guard)
            data["source_floor_guard"] = dict(guard)
            return False

        deficit = max(0.0, misses[0]["floor"] - misses[0]["score"])
        penalty = round(min(0.05, max(0.015, deficit + 0.01)), 4)
        signal.reduce(penalty)
        guard["action"] = "reduce"
        guard["confidence_penalty"] = penalty
        signal.metadata["source_floor_penalty"] = penalty
        signal.metadata["source_floor_guard"] = dict(guard)
        data["source_floor_guard"] = dict(guard)
        return True

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
                risk_multiplier=adaptive_risk_multiplier,
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
            structure = signal.metadata.get("market_structure") if isinstance(signal.metadata.get("market_structure"), dict) else {}
            if signal.direction == "BUY":
                levels = [round(float(level), 6) for level in list(structure.get("bullish_target_levels") or []) if float(level) > entry]
            else:
                levels = [round(float(level), 6) for level in list(structure.get("bearish_target_levels") or []) if float(level) < entry]
            if levels:
                signal.take_profit_levels = levels[:4]
                if signal.direction == "BUY" and tp1 > signal.take_profit_levels[-1]:
                    signal.take_profit_levels.append(round(float(tp1), 6))
                elif signal.direction == "SELL" and tp1 < signal.take_profit_levels[-1]:
                    signal.take_profit_levels.append(round(float(tp1), 6))
                signal.take_profit_levels = list(dict.fromkeys(signal.take_profit_levels))[:4]
                return
            dist = abs(tp1 - entry)
            if dist <= 0:
                return
            if signal.direction == "BUY":
                signal.take_profit_levels = [
                    round(entry + dist * 0.6, 6),
                    round(entry + dist, 6),
                    round(entry + dist * 1.25, 6),
                ]
            else:
                signal.take_profit_levels = [
                    round(entry - dist * 0.6, 6),
                    round(entry - dist, 6),
                    round(entry - dist * 1.25, 6),
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
        if not self._execution_source_floor_gate(signal, context, conf_before, data):
            return False

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
        signal.metadata["memory_same_asset_matches"] = memory.get("same_asset_matches")

        adjustment = float(memory.get("adjustment", 0.0) or 0.0)
        sample_count = int(memory.get("sample_count", 0) or 0)
        same_asset_matches = int(memory.get("same_asset_matches", 0) or 0)
        avg_similarity = float(memory.get("avg_similarity", 0.0) or 0.0)
        memory_edge = float(memory.get("memory_edge", 0.0) or 0.0)
        memory_score = float(memory.get("memory_score", 50.0) or 50.0)
        memory_notes = list(memory.get("notes", []) or [])

        if adjustment > 0:
            signal.boost(adjustment)
        elif adjustment < 0:
            signal.reduce(abs(adjustment))

        signal.metadata["memory_adjustment_applied"] = round(adjustment, 4)
        signal.metadata["memory_notes"] = list(memory_notes)

        strong_negative_memory = _should_kill_for_negative_memory(
            sample_count=sample_count,
            same_asset_matches=same_asset_matches,
            avg_similarity=avg_similarity,
            memory_edge=memory_edge,
            memory_score=memory_score,
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
    def _governance_exception_fallback_verdict(
        signal: Signal,
        *,
        adaptive_policy_preview: Dict[str, Any],
        valid_sources: int,
        min_required: int,
        conf_before: float,
        exc: Exception,
    ) -> Dict[str, Any]:
        min_rr = float(adaptive_policy_preview.get("min_rr", 0.0) or 0.0)
        min_final_confidence = float(
            adaptive_policy_preview.get("min_final_confidence", MIN_FINAL_CONFIDENCE) or MIN_FINAL_CONFIDENCE
        )
        block_new_entries = bool(adaptive_policy_preview.get("block_new_entries"))
        effective_confidence = max(float(signal.confidence or 0.0), float(conf_before or 0.0))
        approved = bool(
            valid_sources >= min_required
            and not block_new_entries
            and float(signal.risk_reward or 0.0) >= min_rr
            and effective_confidence >= min_final_confidence
        )
        return {
            "approved": approved,
            "reason": (
                "governance exception fallback passed"
                if approved
                else (
                    f"governance exception fallback blocked: "
                    f"rr={float(signal.risk_reward or 0.0):.2f}/{min_rr:.2f} "
                    f"conf={effective_confidence:.3f}/{min_final_confidence:.3f} "
                    f"block_new={int(block_new_entries)}"
                )
            ),
            "score": 69 if approved else 0,
            "grade": "C" if approved else "F",
            "model_key": "exception_fallback",
            "live_validation": {},
            "violations": ([] if approved else ["governance_exception_fallback_blocked"]),
            "warnings": [f"governance_exception:{type(exc).__name__}"],
            "fallback": True,
            "exception": type(exc).__name__,
        }

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
            logger.exception(f"[DecisionEngine] Governance exception for {signal.asset}")
            if isinstance(exc, NameError):
                fallback_verdict = SignalDecisionEngine._governance_exception_fallback_verdict(
                    signal,
                    adaptive_policy_preview=adaptive_policy_preview,
                    valid_sources=valid_sources,
                    min_required=min_required,
                    conf_before=conf_before,
                    exc=exc,
                )
                signal.metadata["governance_validation"] = fallback_verdict
                data.update({
                    "grade": fallback_verdict.get("grade"),
                    "score": fallback_verdict.get("score"),
                    "model_key": fallback_verdict.get("model_key"),
                    "live_validation": fallback_verdict.get("live_validation"),
                    "governance_exception_fallback": True,
                    "governance_exception_type": type(exc).__name__,
                })
                if fallback_verdict.get("approved"):
                    signal.step_reached = STEP_GOVERNANCE
                    signal.journal.record(
                        layer=STEP_GOVERNANCE,
                        name="governance",
                        decision=PASS,
                        reason=fallback_verdict.get("reason", "governance exception fallback passed"),
                        conf_before=conf_before,
                        conf_after=signal.confidence,
                        data=data,
                    )
                    return True
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

        try:
            from services.dom_replay_service import get_service as get_dom_replay_service

            get_dom_replay_service().capture_signal_decision(signal, context)
        except Exception as exc:
            logger.debug(f"[DecisionEngine] DOM replay capture failed: {exc}")

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
