from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

from config.config import INACTIVITY_RELIEF_FULL_HOURS, INACTIVITY_RELIEF_START_HOURS


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _active_session(*, category: str = "") -> str:
    now = _utc_now()
    hour = now.hour
    weekday = now.weekday()
    category_key = str(category or "").strip().lower()
    if category_key == "crypto":
        if 0 <= hour < 6:
            return "asia_core"
        if 6 <= hour < 14:
            return "europe_open" if hour < 8 else "europe_core"
        if 14 <= hour < 16:
            return "us_overlap"
        if 16 <= hour < 19:
            return "us_open"
        return "us_core"
    if weekday == 5 or weekday == 6:
        if weekday == 6 and hour >= 22:
            return "asia_core"
        return "off"
    if weekday == 4 and hour >= 22:
        return "off"
    if 0 <= hour < 6:
        return "asia_core"
    if 6 <= hour < 8:
        return "europe_open"
    if 8 <= hour < 13:
        return "europe_core"
    if 13 <= hour < 15:
        return "us_overlap"
    if 15 <= hour < 17:
        return "us_open"
    if 17 <= hour < 22:
        return "us_core"
    return "off"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _context_directional_confluence(
    context: Optional[Dict[str, Any]],
    direction: str,
) -> Dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    direction_sign = _playbook_direction_sign(direction)
    if direction_sign == 0:
        return {
            "score": 0.0,
            "cross_support": 0.0,
            "micro_support": 0.0,
            "whale_support": 0.0,
            "support_components": 0,
            "conflict_components": 0,
            "cross_confidence": 0.0,
            "depth_available": False,
            "synthetic_depth": False,
            "dom_event_backed": False,
            "dom_ladder_ready": False,
            "dom_stream_snapshot_ready": False,
            "dom_source_fidelity": "none",
            "dom_authority_tier": "none",
            "dom_fragmented_market": False,
            "dom_stream_health_score": 1.0,
            "dom_stream_trust_decay": 0.0,
            "dom_stream_degraded": False,
            "depth_update_mode": "none",
            "microstructure_source": "none",
            "depth_provider": "",
            "depth_provider_class": "",
            "depth_environment": "",
            "depth_provider_trust_score": 0.0,
            "depth_quote_alignment_score": 0.0,
            "depth_quote_agreement_state": "",
            "external_depth_rejected": False,
            "depth_levels": 0,
            "depth_quality": 0.0,
            "depth_quality_tier": "none",
            "whale_dominant": "",
            "whale_ratio": 0.0,
        }

    cross = dict(ctx.get("cross_asset_context") or {})
    micro = dict(ctx.get("market_microstructure") or {})
    cross_score = _safe_float(cross.get("score"), 0.0)
    cross_confidence = _clip(_safe_float(cross.get("confidence"), 0.0), 0.0, 1.0)
    cross_state = str(cross.get("state") or "").strip().lower()
    if abs(cross_score) <= 1e-9:
        if cross_state in {"supportive", "buy_support"}:
            cross_score = 0.35
        elif cross_state == "sell_support":
            cross_score = -0.35
        elif cross_state == "conflicted":
            cross_score = -0.25
    cross_support = _clip(cross_score * direction_sign, -1.0, 1.0)

    micro_score = _safe_float(micro.get("score"), 0.0)
    book_imbalance = _safe_float(micro.get("book_imbalance", micro_score), 0.0)
    tick_imbalance = _safe_float(micro.get("tick_imbalance"), 0.0)
    orderflow_score = _safe_float(micro.get("orderflow_score"), 0.0)
    orderflow_book_imbalance = _safe_float(micro.get("orderflow_book_imbalance"), 0.0)
    trade_flow_support = _clip(
        _safe_float(micro.get("trade_flow_score", micro_score), 0.0) * direction_sign,
        -1.0,
        1.0,
    )
    orderflow_support = _clip(
        max(
            orderflow_score * direction_sign,
            orderflow_book_imbalance * direction_sign * 0.85,
        ),
        -1.0,
        1.0,
    )
    velocity_support = _clip(
        (_safe_float(micro.get("velocity_bps"), 0.0) * direction_sign) / 0.45,
        -1.0,
        1.0,
    )
    micro_support = _clip(
        max(
            micro_score * direction_sign,
            trade_flow_support,
            orderflow_support,
            book_imbalance * direction_sign * 0.90,
            tick_imbalance * direction_sign * 0.75,
            velocity_support * 0.85,
        ),
        -1.0,
        1.0,
    )

    whale_dominant = str(ctx.get("whale_dominant") or "").strip().upper()
    whale_ratio = _clip(_safe_float(ctx.get("whale_ratio"), 0.0), 0.0, 1.0)
    whale_sign = 1.0 if whale_dominant == "BUY" else -1.0 if whale_dominant == "SELL" else 0.0
    whale_support = _clip(whale_sign * direction_sign * whale_ratio, -1.0, 1.0)

    depth_available = bool(micro.get("depth_available"))
    synthetic_depth = bool(micro.get("synthetic_depth_available"))
    dom_event_backed = bool(micro.get("dom_event_backed"))
    dom_ladder_ready = bool(micro.get("dom_ladder_ready"))
    dom_stream_snapshot_ready = bool(micro.get("dom_stream_snapshot_ready"))
    dom_source_fidelity = str(micro.get("dom_source_fidelity") or "").strip().lower() or "none"
    dom_authority_tier = str(micro.get("dom_authority_tier") or "").strip().lower() or "none"
    dom_fragmented_market = bool(micro.get("dom_fragmented_market"))
    dom_stream_health_score = _clip(_safe_float(micro.get("dom_stream_health_score"), 1.0), 0.0, 1.0)
    dom_stream_trust_decay = _clip(_safe_float(micro.get("dom_stream_trust_decay"), 0.0), 0.0, 1.0)
    dom_stream_degraded = bool(micro.get("dom_stream_degraded"))
    depth_update_mode = str(micro.get("depth_update_mode") or "").strip().lower() or "none"
    microstructure_source = str(micro.get("microstructure_source") or "").strip().lower() or "none"
    depth_provider = str(
        micro.get("depth_provider")
        or micro.get("provider")
        or micro.get("source")
        or micro.get("exchange")
        or ""
    ).strip()
    depth_provider_key = depth_provider.lower()
    depth_provider_class = str(
        micro.get("depth_provider_class") or micro.get("source_class") or ""
    ).strip().lower()
    depth_environment = str(micro.get("depth_environment") or micro.get("environment") or "").strip().lower()
    depth_provider_trust_score = _clip(_safe_float(micro.get("depth_provider_trust_score"), 0.0), 0.0, 1.0)
    if depth_provider_trust_score <= 0.0:
        if depth_provider_class == "exchange_depth" or any(
            token in depth_provider_key for token in ("binance", "bybit", "okx")
        ):
            depth_provider_trust_score = 0.86
        elif "dukascopy" in depth_provider_key:
            depth_provider_trust_score = 0.92
        elif "ctrader" in depth_provider_key:
            depth_provider_trust_score = 0.58 if depth_environment and depth_environment != "live" else 0.78
        elif depth_provider_class == "redis_subscriber" or "orderflow" in depth_provider_key:
            depth_provider_trust_score = 0.90
    depth_quote_alignment_score = _clip(_safe_float(micro.get("depth_quote_alignment_score"), 0.0), 0.0, 1.0)
    depth_quote_agreement_state = str(micro.get("depth_quote_agreement_state") or "").strip().lower()
    external_depth_rejected = bool(micro.get("external_depth_rejected"))
    depth_levels = int(
        micro.get("depth_levels")
        or max(int(micro.get("bid_level_count", micro.get("visible_bid_levels", 0)) or 0), int(micro.get("ask_level_count", micro.get("visible_ask_levels", 0)) or 0))
        or 0
    )
    depth_quality = _clip(_safe_float(micro.get("depth_quality"), 0.0), 0.0, 1.0)
    depth_quality_tier = str(micro.get("depth_quality_tier") or "").strip().lower() or "none"
    if depth_levels <= 0:
        depth_levels = {
            "full": 10,
            "strong": 8,
            "solid": 6,
            "partial": 4,
            "thin": 2,
            "top_only": 1,
        }.get(depth_quality_tier, 0)
    if depth_quality <= 0.0 and depth_levels > 0:
        depth_quality = (
            1.0
            if depth_levels >= 10
            else 0.82
            if depth_levels >= 8
            else 0.66
            if depth_levels >= 6
            else 0.48
            if depth_levels >= 4
            else 0.30
        )
    if depth_quote_alignment_score <= 0.0 and depth_available and not external_depth_rejected:
        depth_quote_alignment_score = 0.86 if depth_provider_class == "exchange_depth" else 0.80
    if not depth_quote_agreement_state and depth_quote_alignment_score > 0.0:
        depth_quote_agreement_state = "aligned"
    depth_bias = (
        0.10
        if dom_ladder_ready
        else 0.09
        if dom_stream_snapshot_ready
        else 0.08
        if depth_available
        else 0.03
        if synthetic_depth
        else 0.0
    )
    if dom_authority_tier in {"fragmented_event_ladder", "degraded_event_ladder"} or dom_fragmented_market:
        depth_bias = min(depth_bias, 0.06)
    if dom_stream_degraded or dom_stream_health_score < 0.58:
        depth_bias = min(depth_bias, 0.04 if dom_ladder_ready else 0.03 if depth_available else depth_bias)
    depth_bias *= max(0.35, 1.0 - dom_stream_trust_decay * 0.70)

    score = _clip(
        micro_support * 0.46
        + cross_support * 0.34
        + whale_support * 0.20
        + depth_bias,
        -1.0,
        1.0,
    )
    support_components = sum(
        1
        for value in (micro_support, cross_support, whale_support)
        if value >= 0.18
    )
    conflict_components = sum(
        1
        for value in (micro_support, cross_support, whale_support)
        if value <= -0.18
    )
    return {
        "score": round(score, 4),
        "cross_support": round(cross_support, 4),
        "micro_support": round(micro_support, 4),
        "whale_support": round(whale_support, 4),
        "support_components": int(support_components),
        "conflict_components": int(conflict_components),
        "cross_confidence": round(cross_confidence, 4),
        "depth_available": depth_available,
        "synthetic_depth": synthetic_depth,
        "dom_event_backed": dom_event_backed,
        "dom_ladder_ready": dom_ladder_ready,
        "dom_stream_snapshot_ready": dom_stream_snapshot_ready,
        "dom_source_fidelity": dom_source_fidelity,
        "dom_authority_tier": dom_authority_tier,
        "dom_fragmented_market": dom_fragmented_market,
        "dom_stream_health_score": round(dom_stream_health_score, 4),
        "dom_stream_trust_decay": round(dom_stream_trust_decay, 4),
        "dom_stream_degraded": dom_stream_degraded,
        "depth_update_mode": depth_update_mode,
        "microstructure_source": microstructure_source,
        "depth_provider": depth_provider,
        "depth_provider_class": depth_provider_class,
        "depth_environment": depth_environment,
        "depth_provider_trust_score": round(depth_provider_trust_score, 4),
        "depth_quote_alignment_score": round(depth_quote_alignment_score, 4),
        "depth_quote_agreement_state": depth_quote_agreement_state,
        "external_depth_rejected": external_depth_rejected,
        "depth_levels": int(depth_levels),
        "depth_quality": round(depth_quality, 4),
        "depth_quality_tier": depth_quality_tier,
        "whale_dominant": whale_dominant,
        "whale_ratio": round(whale_ratio, 4),
    }


def _shared_shock_profile(
    *,
    candidate: Dict[str, Any],
    structure: Dict[str, Any],
    context: Optional[Dict[str, Any]],
    direction: str,
    category: str,
) -> Dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    direction_sign = _playbook_direction_sign(direction)
    if direction_sign == 0:
        return {
            "score": 0.0,
            "event_score": 0.0,
            "displacement_score": 0.0,
            "structure_score": 0.0,
            "liquidity_score": 0.0,
            "timing_score": 0.0,
            "fresh_event": False,
            "supported": False,
            "timing_intact": False,
            "liquidity_clean": False,
            "event_label": "",
        }

    entry_style = str(candidate.get("entry_style") or "").strip().lower()
    playbook = str(candidate.get("playbook") or "").strip().lower()
    news = dict(ctx.get("news_event") or {})
    sentiment_details = dict(ctx.get("sentiment_details") or {})
    if not sentiment_details and isinstance(ctx.get("market_intelligence"), dict):
        sentiment_details = dict((ctx.get("market_intelligence") or {}).get("sentiment_details") or {})
    headline_shock = dict(sentiment_details.get("headline_shock") or {})
    micro = dict(ctx.get("market_microstructure") or {})
    broker = dict(ctx.get("broker_quality") or {})

    news_state = str(news.get("state") or "").strip().lower()
    news_impact = str(news.get("impact") or "").strip().upper()
    news_direction = str(news.get("direction") or "").strip().upper()
    mins_to_event = abs(_safe_float(news.get("mins_to"), 999.0))
    impact_weight = {"HIGH": 1.0, "MEDIUM": 0.72, "LOW": 0.40}.get(news_impact, 0.0)
    state_weight = (
        1.0
        if news_state == "active"
        else 0.82
        if news_state == "post"
        else 0.30
        if news_state == "pre" and mins_to_event <= 5.0
        else 0.0
    )
    direction_weight = 1.0 if news_direction in {"", direction} else 0.15
    fresh_event_score = _clip(impact_weight * state_weight * direction_weight)
    macro_impact = str(ctx.get("macro_impact") or "").strip().upper()
    macro_score = {"HIGH": 0.60, "MEDIUM": 0.34}.get(macro_impact, 0.0)
    headline_shock_raw_score = _clip(_safe_float(headline_shock.get("score"), 0.0), 0.0, 1.0)
    headline_shock_direction = str(headline_shock.get("direction") or "").strip().upper()
    headline_shock_directional_score = _safe_float(headline_shock.get("directional_score"), 0.0)
    if headline_shock_direction in {"BUY", "SELL"}:
        headline_direction_weight = 1.0 if headline_shock_direction == direction else 0.15
    else:
        headline_direction_weight = 0.45 if headline_shock_raw_score > 0.0 else 0.0
    headline_shock_score = _clip(headline_shock_raw_score * headline_direction_weight, 0.0, 1.0)
    event_score = _clip(max(fresh_event_score, macro_score, headline_shock_score * 0.92))

    alignment_score = _clip(_safe_float(structure.get("alignment_score"), 0.0))
    setup_quality = _clip(_safe_float(structure.get("setup_quality"), 0.0))
    breakout_score = _safe_float(structure.get("breakout_score"), 0.0) * direction_sign
    pullback_score = _safe_float(structure.get("pullback_score"), 0.0) * direction_sign
    candle_quality_score = _clip(_safe_float(structure.get("candle_quality_score"), 0.0))
    session_quality_score = _clip(_safe_float(structure.get("session_quality_score"), 0.0))
    extension_score = max(0.0, _safe_float(structure.get("extension_score"), 0.0))
    target_efficiency_score = _clip(_safe_float(structure.get("target_efficiency_score"), 0.0))
    impulse_age_bars = int(structure.get("impulse_age_bars", 0) or 0)

    break_style = bool(
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
        or playbook in {"aggressive_expansion", "opening_drive", "news_impulse"}
    )
    breakout_component = _clip(max(0.0, breakout_score) * 1.45)
    pullback_component = _clip(max(0.0, pullback_score) * 1.10)

    velocity_support = _clip((_safe_float(micro.get("velocity_bps"), 0.0) * direction_sign) / 0.45)
    trade_flow_support = _clip(_safe_float(micro.get("trade_flow_score", micro.get("score", 0.0)), 0.0) * direction_sign)
    book_support = _clip(_safe_float(micro.get("book_imbalance"), 0.0) * direction_sign)
    tick_support = _clip(_safe_float(micro.get("tick_imbalance"), 0.0) * direction_sign)
    micro_support = _clip(max(trade_flow_support, book_support * 0.85, tick_support * 0.75, velocity_support * 0.85))

    displacement_score = _clip(
        breakout_component * 0.46
        + pullback_component * 0.16
        + velocity_support * 0.20
        + candle_quality_score * 0.12
        + micro_support * 0.06
        + (0.04 if break_style else 0.0)
    )
    structure_score = _clip(
        breakout_component * 0.30
        + pullback_component * 0.12
        + alignment_score * 0.22
        + setup_quality * 0.20
        + target_efficiency_score * 0.10
        + session_quality_score * 0.06
        + (0.05 if break_style else 0.0)
    )

    age_score = 1.0 if impulse_age_bars <= 2 else 0.82 if impulse_age_bars <= 4 else 0.56 if impulse_age_bars <= 6 else 0.24
    extension_tolerance = 1.0 if extension_score <= 0.90 else 0.84 if extension_score <= 1.12 else 0.60 if extension_score <= 1.35 else 0.20
    timing_score = _clip(age_score * 0.55 + extension_tolerance * 0.45)

    spread_regime = str(
        broker.get("spread_regime")
        or micro.get("spread_regime")
        or micro.get("broker_spread_regime")
        or ""
    ).strip().lower()
    quote_quality_state = str(
        broker.get("quote_quality_state")
        or broker.get("agreement_state")
        or micro.get("quote_quality_state")
        or ""
    ).strip().lower()
    spread_bps = _safe_float(broker.get("spread_bps", micro.get("spread_bps", 0.0)), 0.0)
    spread_limit = 40.0 if category == "indices" else 24.0 if category == "commodities" else 18.0 if category == "forex" else 22.0
    spread_score = (
        0.18
        if spread_regime in {"wide", "stressed", "extreme"}
        else 0.56
        if spread_bps > spread_limit
        else 0.74
        if spread_bps > spread_limit * 0.75
        else 1.0
    )
    quote_score = 0.14 if quote_quality_state in {"stale", "delayed", "divergent", "severe_divergence"} else 1.0
    depth_available = bool(micro.get("depth_available"))
    synthetic_depth = bool(micro.get("synthetic_depth_available"))
    depth_score = 0.12 if depth_available else 0.06 if synthetic_depth else 0.0
    liquidity_score = _clip(
        micro_support * 0.48
        + spread_score * 0.26
        + quote_score * 0.18
        + depth_score
    )

    shock_score = _clip(
        event_score * 0.12
        + displacement_score * 0.30
        + structure_score * 0.26
        + liquidity_score * 0.18
        + timing_score * 0.14
    )
    fresh_event = fresh_event_score >= 0.45 or headline_shock_score >= 0.55
    timing_intact = timing_score >= 0.50
    liquidity_clean = liquidity_score >= 0.46 and spread_regime not in {"wide", "stressed", "extreme"} and quote_score >= 0.40
    supported = bool(
        shock_score >= 0.60
        and displacement_score >= 0.58
        and structure_score >= 0.54
        and timing_intact
        and (liquidity_clean or fresh_event or micro_support >= 0.24)
    )
    event_label = f"{news_state}:{news_impact}" if news_state else ""
    if not event_label and headline_shock_raw_score > 0.0:
        event_label = f"headline:{headline_shock_direction or 'NEUTRAL'}"

    return {
        "score": round(shock_score, 4),
        "event_score": round(event_score, 4),
        "headline_shock_score": round(headline_shock_score, 4),
        "headline_shock_raw_score": round(headline_shock_raw_score, 4),
        "headline_shock_direction": headline_shock_direction,
        "headline_shock_directional_score": round(headline_shock_directional_score, 4),
        "headline_shock_direction_weight": round(headline_direction_weight, 4),
        "displacement_score": round(displacement_score, 4),
        "structure_score": round(structure_score, 4),
        "liquidity_score": round(liquidity_score, 4),
        "timing_score": round(timing_score, 4),
        "fresh_event": bool(fresh_event),
        "supported": bool(supported),
        "timing_intact": bool(timing_intact),
        "liquidity_clean": bool(liquidity_clean),
        "event_label": event_label,
    }


def _pattern_family_direction(pattern_family: Any) -> str:
    token = str(pattern_family or "").strip().lower()
    if token.startswith("trending_up_"):
        return "BUY"
    if token.startswith("trending_down_"):
        return "SELL"
    return ""


def _dominant_context_snapshot(context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    buy = _context_directional_confluence(context, "BUY")
    sell = _context_directional_confluence(context, "SELL")

    buy_key = (
        _safe_float(buy.get("score"), 0.0),
        int(buy.get("support_components", 0) or 0),
        abs(_safe_float(buy.get("micro_support"), 0.0)),
        abs(_safe_float(buy.get("cross_support"), 0.0)),
        abs(_safe_float(buy.get("whale_support"), 0.0)),
    )
    sell_key = (
        _safe_float(sell.get("score"), 0.0),
        int(sell.get("support_components", 0) or 0),
        abs(_safe_float(sell.get("micro_support"), 0.0)),
        abs(_safe_float(sell.get("cross_support"), 0.0)),
        abs(_safe_float(sell.get("whale_support"), 0.0)),
    )
    direction = "BUY" if buy_key >= sell_key else "SELL"
    dominant = buy if direction == "BUY" else sell
    dominant_score = _safe_float(dominant.get("score"), 0.0)
    if dominant_score <= 0.0:
        direction = ""
    return {
        "direction": direction,
        "context_confluence": round(max(0.0, dominant_score), 4),
        "cross_alignment": round(_safe_float(dominant.get("cross_support"), 0.0), 4),
        "cross_confidence": round(_safe_float(dominant.get("cross_confidence"), 0.0), 4),
        "micro_score": round(_safe_float(dominant.get("micro_support"), 0.0), 4),
        "whale_context_support": round(_safe_float(dominant.get("whale_support"), 0.0), 4),
        "support_components": int(dominant.get("support_components", 0) or 0),
        "conflict_components": int(dominant.get("conflict_components", 0) or 0),
        "depth_available": bool(dominant.get("depth_available")),
        "synthetic_depth": bool(dominant.get("synthetic_depth")),
        "dom_event_backed": bool(dominant.get("dom_event_backed")),
        "dom_ladder_ready": bool(dominant.get("dom_ladder_ready")),
        "dom_stream_snapshot_ready": bool(dominant.get("dom_stream_snapshot_ready")),
        "dom_source_fidelity": str(dominant.get("dom_source_fidelity") or "none"),
        "depth_update_mode": str(dominant.get("depth_update_mode") or "none"),
        "whale_dominant": str(dominant.get("whale_dominant") or ""),
        "whale_ratio": round(_safe_float(dominant.get("whale_ratio"), 0.0), 4),
    }


def _best_rejected_reason(records: List[Dict[str, Any]]) -> str:
    if not records:
        return ""
    best = max(
        records,
        key=lambda item: (
            _safe_float(item.get("confidence"), 0.0),
            _safe_float(item.get("score"), 0.0),
            1 if str(item.get("reason") or "").strip() else 0,
        ),
    )
    return str(best.get("reason") or "").strip()


def _context_inactivity_profile(context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    adaptive_policy = ctx.get("adaptive_policy")
    if isinstance(adaptive_policy, dict):
        raw_policy = adaptive_policy.get("raw") if isinstance(adaptive_policy.get("raw"), dict) else adaptive_policy
        inactivity = raw_policy.get("inactivity_profile") if isinstance(raw_policy, dict) else None
        if isinstance(inactivity, dict):
            return {
                "active": bool(inactivity.get("active")),
                "hours_since_last_entry": _safe_float(inactivity.get("hours_since_last_entry"), 0.0),
                "relief_strength": _clip(_safe_float(inactivity.get("relief_strength"), 0.0), 0.0, 1.0),
                "flat_book": bool(inactivity.get("flat_book")),
                "open_position_count": int(inactivity.get("open_position_count", 0) or 0),
                "equity_relief": bool(inactivity.get("equity_relief")),
                "equity_relief_strength": _clip(_safe_float(inactivity.get("equity_relief_strength"), 0.0), 0.0, 1.0),
                "category_recent_count": _safe_float(inactivity.get("category_recent_count"), 0.0),
                "asset_recent_count": _safe_float(inactivity.get("asset_recent_count"), 0.0),
            }

    engine = ctx.get("engine")
    state = getattr(engine, "state", None) if engine is not None else ctx.get("state")
    if state is None:
        return {
            "active": False,
            "hours_since_last_entry": 0.0,
            "relief_strength": 0.0,
            "flat_book": False,
            "open_position_count": 0,
            "equity_relief": False,
            "equity_relief_strength": 0.0,
            "category_recent_count": 0.0,
            "asset_recent_count": 0.0,
        }

    try:
        hours_since_last_entry = getattr(state, "hours_since_last_entry", lambda: None)()
        open_position_count = int(getattr(state, "open_position_count", lambda: 0)() or 0)
    except Exception:
        return {
            "active": False,
            "hours_since_last_entry": 0.0,
            "relief_strength": 0.0,
            "flat_book": False,
            "open_position_count": 0,
            "equity_relief": False,
            "equity_relief_strength": 0.0,
            "category_recent_count": 0.0,
            "asset_recent_count": 0.0,
        }

    if hours_since_last_entry is None:
        return {
            "active": False,
            "hours_since_last_entry": 0.0,
            "relief_strength": 0.0,
            "flat_book": open_position_count == 0,
            "open_position_count": open_position_count,
            "equity_relief": False,
            "equity_relief_strength": 0.0,
            "category_recent_count": 0.0,
            "asset_recent_count": 0.0,
        }

    start_hours = max(0.0, float(INACTIVITY_RELIEF_START_HOURS or 0.0))
    full_hours = max(start_hours + 1.0, float(INACTIVITY_RELIEF_FULL_HOURS or 0.0))
    relief_strength = _clip((float(hours_since_last_entry) - start_hours) / max(1.0, full_hours - start_hours), 0.0, 1.0)
    if open_position_count > 0:
        relief_strength *= 0.45
    return {
        "active": bool(relief_strength > 0.0),
        "hours_since_last_entry": round(float(hours_since_last_entry), 2),
        "relief_strength": round(float(relief_strength), 4),
        "flat_book": open_position_count == 0,
        "open_position_count": open_position_count,
        "equity_relief": False,
        "equity_relief_strength": 0.0,
        "category_recent_count": 0.0,
        "asset_recent_count": 0.0,
    }


def _context_participation_relief(
    context: Optional[Dict[str, Any]],
    *,
    asset: str,
    category: str,
) -> Dict[str, Any]:
    ctx = context if isinstance(context, dict) else {}
    engine = ctx.get("engine")
    state = getattr(engine, "state", None) if engine is not None else ctx.get("state")
    if state is None:
        return {
            "active": False,
            "relief_strength": 0.0,
            "category_recent_count": 0.0,
            "asset_recent_count": 0.0,
        }

    try:
        open_positions = list(getattr(state, "get_open_positions", lambda: [])() or [])
    except Exception:
        open_positions = []
    try:
        closed_positions = list(getattr(state, "get_closed_positions", lambda limit=36: [])(36) or [])
    except Exception:
        closed_positions = []

    category_key = str(category or "").strip().lower()
    asset_key = str(asset or "").strip().upper()
    if not category_key or not asset_key:
        return {
            "active": False,
            "relief_strength": 0.0,
            "category_recent_count": 0.0,
            "asset_recent_count": 0.0,
        }

    category_counts: Dict[str, float] = {}
    asset_counts: Dict[str, float] = {}

    def _bump(raw_asset: Any, raw_category: Any, weight: float) -> None:
        local_asset = str(raw_asset or "").strip().upper()
        local_category = str(raw_category or "").strip().lower()
        if local_category:
            category_counts[local_category] = category_counts.get(local_category, 0.0) + float(weight)
        if local_asset:
            asset_counts[local_asset] = asset_counts.get(local_asset, 0.0) + float(weight)

    for pos in open_positions:
        _bump(pos.get("asset") or pos.get("canonical_asset"), pos.get("category"), 1.0)

    for index, trade in enumerate(closed_positions):
        if index < 6:
            weight = 1.0
        elif index < 18:
            weight = 0.55
        else:
            weight = 0.25
        _bump(trade.get("asset") or trade.get("canonical_asset"), trade.get("category"), weight)

    category_recent = float(category_counts.get(category_key, 0.0) or 0.0)
    asset_recent = float(asset_counts.get(asset_key, 0.0) or 0.0)
    max_category_recent = max(category_counts.values()) if category_counts else 0.0
    max_asset_recent = max(asset_counts.values()) if asset_counts else 0.0
    if max_category_recent < 2.0:
        return {
            "active": False,
            "relief_strength": 0.0,
            "category_recent_count": round(category_recent, 4),
            "asset_recent_count": round(asset_recent, 4),
        }

    category_gap = max(0.0, (max_category_recent - category_recent) / max(max_category_recent, 1.0))
    asset_gap = 0.0
    if max_asset_recent > 0.0:
        asset_gap = max(0.0, (max_asset_recent - asset_recent) / max(max_asset_recent, 1.0))
    relief_strength = _clip(category_gap * 0.78 + asset_gap * 0.22, 0.0, 1.0)
    return {
        "active": bool(relief_strength >= 0.18),
        "relief_strength": round(float(relief_strength), 4),
        "category_recent_count": round(category_recent, 4),
        "asset_recent_count": round(asset_recent, 4),
    }


def _merge_context_relief_profiles(
    base_profile: Optional[Dict[str, Any]],
    participation_relief: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    profile = dict(base_profile or {})
    participation = participation_relief if isinstance(participation_relief, dict) else {}
    category_recent_count = round(_safe_float(participation.get("category_recent_count"), 0.0), 4)
    asset_recent_count = round(_safe_float(participation.get("asset_recent_count"), 0.0), 4)
    raw_equity_relief_strength = _clip(_safe_float(participation.get("relief_strength"), 0.0), 0.0, 1.0)
    equity_relief = bool(participation.get("active")) and raw_equity_relief_strength > 0.0
    equity_relief_strength = (
        _clip(0.12 + raw_equity_relief_strength * 0.88, 0.0, 1.0)
        if equity_relief
        else 0.0
    )

    profile["equity_relief"] = equity_relief
    profile["equity_relief_strength"] = round(float(equity_relief_strength), 4)
    profile["category_recent_count"] = category_recent_count
    profile["asset_recent_count"] = asset_recent_count

    if equity_relief:
        current_relief_strength = _clip(_safe_float(profile.get("relief_strength"), 0.0), 0.0, 1.0)
        profile["active"] = True
        profile["relief_strength"] = round(max(current_relief_strength, equity_relief_strength), 4)

    return profile


def _session_matches(current: str, allowed: str) -> bool:
    current_label = str(current or "").strip().lower()
    allowed_label = str(allowed or "").strip().lower()
    if not current_label or not allowed_label:
        return False
    if current_label == allowed_label:
        return True

    broad_windows = {
        "asia": {"asia_core"},
        "europe": {"europe_open", "europe_core", "us_overlap"},
        "us": {"us_overlap", "us_open", "us_core"},
    }
    if allowed_label in broad_windows:
        return current_label in broad_windows[allowed_label]
    return False


def _news_direction_sign(raw_direction: Any) -> int:
    label = str(raw_direction or "").strip().lower()
    if label in {"buy", "bullish", "up", "long", "risk_on"}:
        return 1
    if label in {"sell", "bearish", "down", "short", "risk_off"}:
        return -1
    return 0


def _playbook_direction_sign(direction: str) -> int:
    label = str(direction or "").strip().upper()
    if label == "BUY":
        return 1
    if label == "SELL":
        return -1
    return 0


def _candidate_threshold_reason(value: float, floor: float, reason: str, playbook: str) -> str:
    if value < floor:
        return f"{reason}:{playbook}"
    return ""


def _candidate_exhaustion_reason(
    direction: str,
    upside_exhaustion_score: float,
    downside_exhaustion_score: float,
    threshold: float,
    playbook: str,
) -> str:
    if direction == "BUY" and upside_exhaustion_score >= threshold:
        return f"upside_exhausted:{playbook}"
    if direction == "SELL" and downside_exhaustion_score >= threshold:
        return f"downside_exhausted:{playbook}"
    return ""


def _candidate_bias_conflict_reason(structure_bias: str, bias_alignment: bool, strong_break: bool, playbook: str) -> str:
    if structure_bias in {"buy", "sell"} and not bias_alignment and not strong_break:
        return f"bias_conflict:{playbook}"
    return ""


def _candidate_trend_misaligned_reason(
    aligned_trends: int,
    required_trends: int,
    strong_break: bool,
    playbook: str,
) -> str:
    if aligned_trends < max(0, int(required_trends or 0)) and not strong_break:
        return f"trend_misaligned:{playbook}"
    return ""


def _effective_confirmation_gate(
    *,
    playbook: str,
    entry_confirmation_ready: bool,
    entry_confirmation_count: int,
    entry_confirmation_bars_required: int,
    fast_entry_confirmation_ready: bool = False,
    fast_entry_confirmation_count: int = 0,
    fast_entry_confirmation_bars_required: int = 0,
) -> tuple[bool, int, int, bool]:
    raw_ready = bool(entry_confirmation_ready)
    raw_count = max(0, int(entry_confirmation_count or 0))
    raw_required = max(0, int(entry_confirmation_bars_required or 0))
    fast_ready = bool(fast_entry_confirmation_ready)
    fast_count = max(0, int(fast_entry_confirmation_count or 0))
    fast_required = max(0, int(fast_entry_confirmation_bars_required or 0))

    if playbook in _PREMIUM_ENTRY_PLAYBOOKS:
        return raw_ready, raw_count, raw_required, False
    if playbook in _FAST_ENTRY_PLAYBOOKS:
        effective_required = fast_required or raw_required
        effective_count = max(raw_count, fast_count)
        effective_ready = raw_ready or fast_ready
        fast_override = bool(fast_ready and not raw_ready)
        return effective_ready, effective_count, effective_required, fast_override
    return raw_ready, raw_count, raw_required, False


def _near_confirmation(count: int, required: int) -> bool:
    required = max(0, int(required or 0))
    if required <= 0:
        return False
    return max(0, int(count or 0)) >= max(1, required - 1)


def _qualify_crypto_orderflow_candidate(
    *,
    candidate: Dict[str, Any],
    profile: _PlaybookProfile,
    plan: _AssetPlaybookPlan,
    playbook: str,
    direction: str,
    structure_bias: str,
    alignment_score: float,
    setup_quality: float,
    upside_exhaustion_score: float,
    downside_exhaustion_score: float,
    aligned_trends: int,
    bias_alignment: bool,
) -> tuple[bool, str, bool]:
    candidate_score = _safe_float(candidate.get("score", 0.0), 0.0)
    imbalance_strength = abs(_safe_float(candidate.get("book_imbalance", 0.0), 0.0))
    micro_strength = abs(_safe_float(candidate.get("micro_score", 0.0), 0.0))
    strong_micro_break = (
        candidate_score >= max(profile.breakout_min_score, 0.60)
        and imbalance_strength >= 0.38
        and micro_strength >= 0.28
    )
    relaxed_alignment_floor = 0.0 if strong_micro_break else max(0.25, float(plan.min_alignment_score) - 0.18)
    relaxed_setup_floor = max(0.12, float(plan.min_setup_quality) - (0.42 if strong_micro_break else 0.12))

    reason = _candidate_threshold_reason(alignment_score, relaxed_alignment_floor, "alignment_too_weak", playbook)
    if reason:
        return False, reason, strong_micro_break
    reason = _candidate_threshold_reason(setup_quality, relaxed_setup_floor, "setup_quality_too_weak", playbook)
    if reason:
        return False, reason, strong_micro_break
    reason = _candidate_exhaustion_reason(direction, upside_exhaustion_score, downside_exhaustion_score, 0.72, playbook)
    if reason:
        return False, reason, strong_micro_break
    reason = _candidate_bias_conflict_reason(structure_bias, bias_alignment, strong_micro_break, playbook)
    if reason:
        return False, reason, strong_micro_break
    reason = _candidate_trend_misaligned_reason(aligned_trends, 1, strong_micro_break, playbook)
    if reason:
        return False, reason, strong_micro_break
    return True, "", strong_micro_break


def _qualify_impulse_candidate(
    *,
    candidate: Dict[str, Any],
    profile: _PlaybookProfile,
    plan: _AssetPlaybookPlan,
    playbook: str,
    direction: str,
    structure_bias: str,
    alignment_score: float,
    setup_quality: float,
    upside_exhaustion_score: float,
    downside_exhaustion_score: float,
    aligned_trends: int,
    bias_alignment: bool,
    entry_style: str = "",
    pattern_family: str = "",
    entry_confirmation_ready: bool = False,
    entry_confirmation_count: int = 0,
    entry_confirmation_bars_required: int = 0,
    target_efficiency_score: float = 0.0,
    extension_score: float = 0.0,
    impulse_age_bars: int = 0,
    elite_pattern_rank: float = 0.0,
    cluster_penalty: float = 0.0,
    has_execution_structure: bool = False,
    liquidity_sweep_directional: bool = False,
    preferred_interval: str = "",
    trigger_trend_aligned: bool = False,
    structure_promoted: bool = False,
    external_confirmation_score: float = 0.0,
    inactivity_relief_strength: float = 0.0,
    inactivity_flat_book: bool = False,
    shock_score: float = 0.0,
    shock_event_score: float = 0.0,
    shock_displacement_score: float = 0.0,
    shock_structure_score: float = 0.0,
    shock_liquidity_score: float = 0.0,
    shock_timing_score: float = 0.0,
    shock_fresh_event: bool = False,
    shock_supported: bool = False,
    depth_context_ready: bool = False,
    context_confluence_score: float = 0.0,
    context_support_components: int = 0,
    context_conflict_components: int = 0,
    context_micro_support: float = 0.0,
    context_cross_support: float = 0.0,
    context_whale_support: float = 0.0,
) -> tuple[bool, str, bool, bool]:
    candidate_score = _safe_float(candidate.get("score", 0.0), 0.0)
    entry_style_label = str(entry_style or "").strip().lower()
    context_score = _safe_float(context_confluence_score, 0.0)
    support_count = max(
        int(candidate.get("support_components", 0) or 0),
        int(context_support_components or 0),
    )
    conflict_count = max(
        int(candidate.get("conflict_components", 0) or 0),
        int(context_conflict_components or 0),
    )
    cross_strength = max(
        abs(_safe_float(candidate.get("cross_alignment", 0.0), 0.0)),
        abs(_safe_float(context_cross_support, 0.0)),
    )
    micro_strength = max(
        abs(_safe_float(candidate.get("micro_score", 0.0), 0.0)),
        abs(_safe_float(context_micro_support, 0.0)),
    )
    whale_strength = abs(_safe_float(context_whale_support, 0.0))
    depth_context_confirmation = bool(
        depth_context_ready
        and support_count >= 1
        and conflict_count == 0
        and context_score >= 0.30
        and max(micro_strength, cross_strength, whale_strength) >= 0.34
        and target_efficiency_score >= 0.50
        and extension_score <= 1.35
    )
    impulse_break_style = bool(
        entry_style_label in {
            "expansion_break",
            "opening_drive_break",
            "news_followthrough",
            "intermarket_break",
            "intermarket_confirmed_break",
            "breakout_close",
            "breakout_ignition",
        }
        or (
            entry_style_label.endswith("_break")
            and "pullback" not in entry_style_label
            and "retest" not in entry_style_label
        )
    )
    impulse_floor = {
        "aggressive_expansion": max(profile.expansion_min_score, 0.68),
        "breakout_continuation": max(profile.breakout_min_score, 0.66),
        "opening_drive": max(profile.breakout_min_score, 0.60),
        "news_impulse": max(profile.breakout_min_score, 0.62),
        "intermarket_continuation": max(profile.breakout_min_score, 0.60),
    }.get(playbook, 0.68)
    strong_impulse_break = candidate_score >= impulse_floor
    if (
        playbook == "intermarket_continuation"
        and candidate_score >= max(profile.breakout_min_score, 0.56)
        and max(cross_strength, micro_strength) >= 0.28
    ):
        strong_impulse_break = True
    family = str(pattern_family or "").lower()
    near_confirmation = _near_confirmation(entry_confirmation_count, entry_confirmation_bars_required)
    early_relief_candidate_floor = max(0.50, float(profile.breakout_min_score) - 0.08)
    if entry_style_label == "elite_sweep_continuation":
        early_relief_candidate_floor = max(0.30, float(profile.breakout_min_score) - 0.28)
    elif entry_style_label == "elite_trend_continuation":
        early_relief_candidate_floor = max(0.38, float(profile.breakout_min_score) - 0.18)
    elif entry_style_label == "opening_drive_break":
        early_relief_candidate_floor = max(0.40, float(profile.breakout_min_score) - 0.18)
    elif entry_style_label == "expansion_break":
        early_relief_candidate_floor = max(0.42, float(profile.breakout_min_score) - 0.16)
    elif entry_style_label == "news_followthrough":
        early_relief_candidate_floor = max(0.42, float(profile.breakout_min_score) - 0.16)
    elif entry_style_label == "breakout_ignition":
        early_relief_candidate_floor = max(0.38, float(profile.breakout_min_score) - 0.22)
    elif entry_style_label.startswith("intermarket_") and "pullback" not in entry_style_label and "retest" not in entry_style_label:
        early_relief_candidate_floor = max(0.40, float(profile.breakout_min_score) - 0.18)
    elif family.endswith("liquidity_sweep"):
        early_relief_candidate_floor = max(0.44, float(profile.breakout_min_score) - 0.18)
    elif family.endswith("first_pullback") or family.endswith("breakout_retest"):
        early_relief_candidate_floor = max(0.47, float(profile.breakout_min_score) - 0.12)
    fast_interval = str(preferred_interval or "").strip().lower() in {"1m", "5m", "15m"}
    fast_supportive_context = bool(
        external_confirmation_score >= 0.16
        or structure_promoted
        or trigger_trend_aligned
        or max(cross_strength, micro_strength) >= 0.22
        or depth_context_confirmation
    )
    inactivity_seed_relief = bool(inactivity_flat_book and inactivity_relief_strength > 0.0)
    inactivity_candidate_floor = max(
        0.44,
        early_relief_candidate_floor - (0.05 + inactivity_relief_strength * 0.10),
    )
    inactivity_alignment_floor = max(
        0.46,
        float(plan.min_alignment_score) - (0.08 + inactivity_relief_strength * 0.10),
    )
    inactivity_setup_floor = max(
        0.44,
        float(plan.min_setup_quality) - (0.06 + inactivity_relief_strength * 0.08),
    )
    inactivity_fast_track_context = bool(
        inactivity_seed_relief
        and fast_interval
        and bias_alignment
        and candidate_score >= inactivity_candidate_floor
        and alignment_score >= inactivity_alignment_floor
        and setup_quality >= inactivity_setup_floor
        and (
            family.endswith("liquidity_sweep")
            or family.endswith("first_pullback")
            or family.endswith("breakout_retest")
            or family.endswith("generic")
            or near_confirmation
            or fast_supportive_context
        )
    )
    fast_impulse_context = bool(
        playbook in {"breakout_continuation", "intermarket_continuation", "aggressive_expansion", "opening_drive", "news_impulse"}
        and impulse_break_style
        and fast_interval
        and bias_alignment
        and candidate_score >= early_relief_candidate_floor
        and alignment_score >= max(0.50, float(plan.min_alignment_score) - 0.10)
        and setup_quality >= max(0.48, float(plan.min_setup_quality) - 0.08)
        and fast_supportive_context
    )
    fast_shock_context = bool(
        playbook in {"breakout_continuation", "intermarket_continuation", "aggressive_expansion", "opening_drive", "news_impulse"}
        and impulse_break_style
        and bias_alignment
        and shock_supported
        and shock_score >= 0.60
        and shock_displacement_score >= 0.58
        and shock_structure_score >= 0.54
        and shock_timing_score >= 0.50
        and candidate_score >= max(0.40, early_relief_candidate_floor - 0.10)
        and alignment_score >= max(0.48, float(plan.min_alignment_score) - 0.12)
        and setup_quality >= max(0.46, float(plan.min_setup_quality) - 0.10)
        and (
            shock_liquidity_score >= 0.42
            or shock_fresh_event
            or fast_supportive_context
        )
    )
    allow_early_trend_relief = bool(
        playbook in {"breakout_continuation", "intermarket_continuation", "aggressive_expansion", "opening_drive", "news_impulse"}
        and fast_interval
        and bias_alignment
        and candidate_score >= early_relief_candidate_floor
        and alignment_score >= max(0.52, float(plan.min_alignment_score) - 0.08)
        and setup_quality >= max(0.50, float(plan.min_setup_quality) - 0.06)
        and (
            (family.endswith("liquidity_sweep") and (liquidity_sweep_directional or bias_alignment))
            or (family.endswith("first_pullback") and near_confirmation)
            or (family.endswith("breakout_retest") and near_confirmation)
            or (entry_confirmation_ready and near_confirmation)
            or fast_supportive_context
        )
    )
    if fast_impulse_context:
        strong_impulse_break = True
        allow_early_trend_relief = True
    if fast_shock_context:
        strong_impulse_break = True
        allow_early_trend_relief = True
    if inactivity_fast_track_context:
        allow_early_trend_relief = True
    context_pressure_ready = bool(
        entry_style_label == "elite_context_pressure"
        and candidate_score >= max(0.44, float(profile.breakout_min_score) - 0.16)
        and max(cross_strength, micro_strength) >= 0.28
        and support_count >= 1
        and conflict_count == 0
    )
    if context_pressure_ready:
        strong_impulse_break = True
        allow_early_trend_relief = True
    breakout_ignition_context = bool(
        entry_style_label == "breakout_ignition"
        and candidate_score >= early_relief_candidate_floor
        and support_count >= 1
        and conflict_count == 0
        and max(cross_strength, micro_strength) >= 0.22
        and target_efficiency_score >= 0.08
        and extension_score <= 1.62
        and impulse_age_bars <= 8
    )
    if breakout_ignition_context:
        strong_impulse_break = True
        allow_early_trend_relief = True
    if allow_early_trend_relief:
        strong_impulse_break = True

    if playbook == "aggressive_expansion" and has_execution_structure:
        expansion_confirmed = bool(
            entry_confirmation_ready
            or near_confirmation
            or fast_shock_context
            or depth_context_confirmation
            or (
                external_confirmation_score >= 0.20
                and max(cross_strength, micro_strength) >= 0.18
            )
            or (
                structure_promoted
                and target_efficiency_score >= 0.24
                and elite_pattern_rank >= 0.08
            )
        )
        expansion_context_support = bool(
            elite_pattern_rank >= 0.10
            or external_confirmation_score >= 0.22
            or max(cross_strength, micro_strength) >= 0.28
            or shock_supported
            or depth_context_confirmation
        )
        if entry_confirmation_bars_required > 0 and not expansion_confirmed:
            return False, f"confirmation_pending:{playbook}", strong_impulse_break, allow_early_trend_relief
        if impulse_age_bars >= 6 and extension_score >= 1.20 and not fast_shock_context:
            return False, f"stale_or_stretched:{playbook}", strong_impulse_break, allow_early_trend_relief
        if target_efficiency_score <= 0.10 and not fast_shock_context:
            return False, f"target_space_too_thin:{playbook}", strong_impulse_break, allow_early_trend_relief
        if elite_pattern_rank <= 0.02 and not expansion_context_support:
            return False, f"pattern_rank_too_weak:{playbook}", strong_impulse_break, allow_early_trend_relief
        if cluster_penalty >= 0.26 and not fast_shock_context:
            return False, f"cluster_risk_too_high:{playbook}", strong_impulse_break, allow_early_trend_relief

    effective_required_trends = int(plan.min_trend_agreement or 0)
    if allow_early_trend_relief:
        effective_required_trends = min(effective_required_trends, 1)
    relaxed_alignment_floor = 0.0 if strong_impulse_break else max(
        0.22,
        float(plan.min_alignment_score) - 0.16 - (inactivity_relief_strength * 0.06 if inactivity_seed_relief else 0.0),
    )
    relaxed_setup_floor = max(
        0.10,
        float(plan.min_setup_quality)
        - (0.42 if strong_impulse_break else 0.10)
        - (inactivity_relief_strength * 0.05 if inactivity_seed_relief else 0.0),
    )

    reason = _candidate_threshold_reason(alignment_score, relaxed_alignment_floor, "alignment_too_weak", playbook)
    if reason:
        return False, reason, strong_impulse_break, allow_early_trend_relief
    reason = _candidate_threshold_reason(setup_quality, relaxed_setup_floor, "setup_quality_too_weak", playbook)
    if reason:
        return False, reason, strong_impulse_break, allow_early_trend_relief
    exhaustion_limit = 0.72 if strong_impulse_break else 0.62
    reason = _candidate_exhaustion_reason(direction, upside_exhaustion_score, downside_exhaustion_score, exhaustion_limit, playbook)
    if reason:
        return False, reason, strong_impulse_break, allow_early_trend_relief
    reason = _candidate_bias_conflict_reason(structure_bias, bias_alignment, strong_impulse_break, playbook)
    if reason:
        return False, reason, strong_impulse_break, allow_early_trend_relief
    reason = _candidate_trend_misaligned_reason(aligned_trends, effective_required_trends, strong_impulse_break, playbook)
    if reason:
        return False, reason, strong_impulse_break, allow_early_trend_relief
    return True, "", strong_impulse_break, allow_early_trend_relief


def _qualify_standard_candidate(
    *,
    playbook: str,
    plan: _AssetPlaybookPlan,
    alignment_score: float,
    setup_quality: float,
    inactivity_relief_strength: float = 0.0,
    inactivity_flat_book: bool = False,
    context_confluence: float = 0.0,
) -> tuple[bool, str]:
    inactivity_seed_relief = bool(inactivity_flat_book and inactivity_relief_strength > 0.0)
    alignment_floor = float(plan.min_alignment_score)
    setup_floor = float(plan.min_setup_quality)
    if inactivity_seed_relief:
        alignment_floor = max(0.50, alignment_floor - (0.03 + inactivity_relief_strength * 0.05))
        setup_floor = max(0.48, setup_floor - (0.03 + inactivity_relief_strength * 0.05))
    if playbook in _REVERSAL_PLAYBOOKS and context_confluence >= 0.18:
        alignment_floor = max(0.44, alignment_floor - 0.12)
        setup_floor = max(0.42, setup_floor - 0.10)
    reason = _candidate_threshold_reason(alignment_score, alignment_floor, "alignment_too_weak", playbook)
    if reason:
        return False, reason
    reason = _candidate_threshold_reason(setup_quality, setup_floor, "setup_quality_too_weak", playbook)
    if reason:
        return False, reason
    return True, ""


def _qualify_family_rules(
    *,
    playbook: str,
    plan: _AssetPlaybookPlan,
    structure_bias: str,
    bias_alignment: bool,
    aligned_trends: int,
    opposing_trends: int,
    upside_exhaustion_score: float,
    downside_exhaustion_score: float,
    strong_impulse_break: bool,
    direction: str,
    allow_early_trend_relief: bool = False,
    reversal_context_support: float = 0.0,
    reversal_support_components: int = 0,
    reversal_candidate_score: float = 0.0,
) -> str:
    if playbook in _TREND_PLAYBOOKS and playbook != "crypto_orderflow_continuation":
        required_trends = int(plan.min_trend_agreement or 0)
        if allow_early_trend_relief:
            required_trends = min(required_trends, 1)
        if required_trends >= 2 and structure_bias in {"buy", "sell"} and aligned_trends < required_trends:
            return _candidate_trend_misaligned_reason(aligned_trends, required_trends, False, playbook)
        if not strong_impulse_break:
            reason = _candidate_exhaustion_reason(direction, upside_exhaustion_score, downside_exhaustion_score, 0.62, playbook)
            if reason:
                return reason
            reason = _candidate_bias_conflict_reason(structure_bias, bias_alignment, False, playbook)
            if reason:
                return reason
            return _candidate_trend_misaligned_reason(aligned_trends, required_trends, False, playbook)

    if playbook in _EARLY_INFLECTION_PLAYBOOKS:
        if structure_bias in {"buy", "sell"} and bias_alignment:
            return f"inflection_not_countertrend:{playbook}"
        if direction == "SELL" and upside_exhaustion_score < 0.42:
            return f"inflection_not_exhausted:{playbook}"
        if direction == "BUY" and downside_exhaustion_score < 0.42:
            return f"inflection_not_exhausted:{playbook}"
        if opposing_trends < 1:
            return f"inflection_not_early:{playbook}"

    if playbook in _REVERSAL_PLAYBOOKS:
        if structure_bias in {"buy", "sell"} and bias_alignment:
            return f"reversal_not_countertrend:{playbook}"
        reversal_early_relief = bool(
            reversal_context_support >= 0.18
            and reversal_support_components >= 1
            and reversal_candidate_score >= 0.52
        )
        required_opposing_trends = max(0, int(plan.reversal_min_opposing_trend_agreement or 0))
        if reversal_early_relief:
            required_opposing_trends = max(0, required_opposing_trends - 1)
        if opposing_trends < required_opposing_trends:
            return f"reversal_unconfirmed:{playbook}"

    return ""


def _elite_entry_gate_reason(*, playbook: str, structure: Dict[str, Any], candidate: Optional[Dict[str, Any]] = None) -> str:
    breakout_retest_ready = bool(structure.get("breakout_retest_ready"))
    first_pullback_ready = bool(structure.get("first_pullback_ready"))
    failed_opposite_move_confirmed = bool(structure.get("failed_opposite_move_confirmed"))
    reclaim_confirmed = bool((candidate or {}).get("reclaim_confirmed"))
    retest_confirmed = bool((candidate or {}).get("retest_confirmed"))
    pullback_confirmed = bool((candidate or {}).get("pullback_confirmed"))

    if playbook == "breakout_retest" and not (breakout_retest_ready or retest_confirmed):
        return "retest_missing:breakout_retest"
    if playbook == "trend_pullback" and not (first_pullback_ready or pullback_confirmed):
        return "pullback_missing:trend_pullback"
    if playbook == "failed_break_reclaim" and not (failed_opposite_move_confirmed or reclaim_confirmed):
        return "reclaim_unconfirmed:failed_break_reclaim"
    return ""


def _build_early_inflection_candidate(
    *,
    direction: str,
    structure_bias: str,
    latest_open: float,
    latest_close: float,
    latest_high: float,
    latest_low: float,
    prev_close: float,
    range_high: float,
    range_low: float,
    atr: float,
    avg_body: float,
    setup_quality: float,
    alignment_score: float,
    regime: str,
    upside_exhaustion_score: float,
    downside_exhaustion_score: float,
    profile: _PlaybookProfile,
    preferred_interval: str,
    management: Dict[str, Any],
    asset: str,
    category: str,
    session: str,
) -> Optional[Dict[str, Any]]:
    if direction == "SELL":
        if structure_bias != "buy":
            return None
        if latest_close >= latest_open or latest_close >= prev_close:
            return None
        if upside_exhaustion_score < 0.34:
            return None
        rejection_body = _clip((latest_open - latest_close) / max(avg_body * 1.6, 1e-9))
        close_off_high = _clip((latest_high - latest_close) / max(atr * 0.95, 1e-9))
        near_extreme = _clip(1.0 - max(0.0, range_high - latest_high) / max(atr * 0.9, 1e-9))
        momentum_flip = _clip((prev_close - latest_close) / max(atr * 0.85, 1e-9))
        regime_bonus = 0.08 if regime in {"trending_up", "volatile"} else 0.03
        score = (
            _clip(upside_exhaustion_score) * 0.24
            + rejection_body * 0.18
            + close_off_high * 0.18
            + near_extreme * 0.14
            + momentum_flip * 0.12
            + _clip(setup_quality) * 0.08
            + _clip(alignment_score) * 0.06
            + regime_bonus
        )
        if score < max(profile.reversal_min_score - 0.03, 0.54):
            return None
        confidence = _clip(0.41 + score * 0.40, 0.0, 0.92)
        notes = [
            "early_inflection",
            "uptrend_rollover",
            "early_bearish_turn",
            f"session={session}",
        ]
    else:
        if structure_bias != "sell":
            return None
        if latest_close <= latest_open or latest_close <= prev_close:
            return None
        if downside_exhaustion_score < 0.34:
            return None
        rejection_body = _clip((latest_close - latest_open) / max(avg_body * 1.6, 1e-9))
        close_off_low = _clip((latest_close - latest_low) / max(atr * 0.95, 1e-9))
        near_extreme = _clip(1.0 - max(0.0, latest_low - range_low) / max(atr * 0.9, 1e-9))
        momentum_flip = _clip((latest_close - prev_close) / max(atr * 0.85, 1e-9))
        regime_bonus = 0.08 if regime in {"trending_down", "volatile"} else 0.03
        score = (
            _clip(downside_exhaustion_score) * 0.24
            + rejection_body * 0.18
            + close_off_low * 0.18
            + near_extreme * 0.14
            + momentum_flip * 0.12
            + _clip(setup_quality) * 0.08
            + _clip(alignment_score) * 0.06
            + regime_bonus
        )
        if score < max(profile.reversal_min_score - 0.03, 0.54):
            return None
        confidence = _clip(0.41 + score * 0.40, 0.0, 0.92)
        notes = [
            "early_inflection",
            "downtrend_turn",
            "early_bullish_turn",
            f"session={session}",
        ]

    return {
        "playbook": "early_inflection",
        "direction": direction,
        "score": round(score, 4),
        "confidence": round(confidence, 4),
        "entry_style": "early_inflection_turn",
        "session": session,
        "preferred_interval": preferred_interval,
        "management": management,
        "notes": notes,
    }


@dataclass(frozen=True)
class _PlaybookProfile:
    breakout_min_score: float
    pullback_min_score: float
    retest_min_score: float
    reversal_min_score: float
    expansion_min_score: float
    seed_min_confidence: float
    support_min_confidence: float
    override_min_confidence: float
    override_gap: float
    weak_ml_confidence: float
    breakout_lookback: int
    preferred_interval: str
    allowed_sessions: tuple[str, ...]
    retest_window: int
    retest_tolerance_atr: float
    runner_target_rr: float
    trail_activation_rr: float
    trail_atr_multiple: float


@dataclass(frozen=True)
class _AssetPlaybookPlan:
    allowed_playbooks: tuple[str, ...]
    allowed_sessions: tuple[str, ...]
    min_alignment_score: float
    min_setup_quality: float
    min_trend_agreement: int
    reversal_min_opposing_trend_agreement: int


@dataclass(frozen=True)
class _SeedState:
    direction: str
    direction_source: str
    structure_bias: str
    pattern_family: str
    pattern_family_direction: str
    alignment_score: float
    setup_quality: float
    pullback_score: float
    breakout_score: float
    extension_score: float
    target_efficiency_score: float
    elite_pattern_rank: float
    first_pullback_ready: bool
    breakout_retest_ready: bool
    failed_opposite_move_confirmed: bool
    entry_confirmation_ready: bool
    fast_entry_confirmation_ready: bool
    context_confluence: float
    cross_alignment: float
    cross_confidence: float
    micro_score: float
    whale_context_support: float
    support_components: int
    conflict_components: int
    context_pressure_ready: bool
    context_driven_direction: bool
    strong_generic_pattern_ready: bool
    pattern_driven_direction: bool
    neutral_pattern_pullback_ready: bool


def _build_seed_state(
    *,
    structure: Dict[str, Any],
    plan: _AssetPlaybookPlan,
    context: Optional[Dict[str, Any]] = None,
    allow_breakout_direction: bool = False,
) -> _SeedState:
    structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
    pattern_family = str(structure.get("pattern_family", "unknown") or "unknown").lower()
    pattern_family_direction = _pattern_family_direction(pattern_family)
    alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
    setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
    pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
    breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
    extension_score = float(structure.get("extension_score", 0.0) or 0.0)
    target_efficiency_score = float(structure.get("target_efficiency_score", 0.0) or 0.0)
    elite_pattern_rank = float(structure.get("elite_pattern_rank", 0.0) or 0.0)
    first_pullback_ready = bool(structure.get("first_pullback_ready"))
    breakout_retest_ready = bool(structure.get("breakout_retest_ready"))
    failed_opposite_move_confirmed = bool(structure.get("failed_opposite_move_confirmed"))
    entry_confirmation_ready = bool(structure.get("entry_confirmation_ready"))
    fast_entry_confirmation_ready = bool(structure.get("fast_entry_confirmation_ready"))

    dominant_context = _dominant_context_snapshot(context)
    context_direction = str(dominant_context.get("direction") or "").upper()
    context_confluence = _safe_float(dominant_context.get("context_confluence"), 0.0)
    support_components = int(dominant_context.get("support_components", 0) or 0)
    conflict_components = int(dominant_context.get("conflict_components", 0) or 0)
    cross_alignment = _safe_float(dominant_context.get("cross_alignment"), 0.0)
    cross_confidence = _safe_float(dominant_context.get("cross_confidence"), 0.0)
    micro_score = _safe_float(dominant_context.get("micro_score"), 0.0)
    whale_context_support = _safe_float(dominant_context.get("whale_context_support"), 0.0)

    context_pressure_ready = bool(
        context_direction in {"BUY", "SELL"}
        and context_confluence >= 0.24
        and support_components >= 1
        and conflict_components == 0
        and (
            micro_score >= 0.22
            or cross_alignment >= 0.22
            or whale_context_support >= 0.28
        )
    )
    context_driven_direction = bool(structure_bias not in {"buy", "sell"} and context_pressure_ready)

    direction = ""
    direction_source = ""
    if structure_bias in {"buy", "sell"}:
        direction = "BUY" if structure_bias == "buy" else "SELL"
        direction_source = "structure"
    elif context_driven_direction:
        direction = context_direction
        direction_source = "context"
    elif pattern_family_direction in {"BUY", "SELL"}:
        direction = pattern_family_direction
        direction_source = "pattern"
    elif allow_breakout_direction and breakout_score >= 0.18:
        direction = "BUY"
        direction_source = "breakout"
    elif allow_breakout_direction and breakout_score <= -0.18:
        direction = "SELL"
        direction_source = "breakout"

    strong_generic_pattern_ready = bool(
        direction in {"BUY", "SELL"}
        and pattern_family.endswith("generic")
        and pattern_family_direction == direction
        and alignment_score >= max(0.62, float(plan.min_alignment_score))
        and setup_quality >= max(0.58, float(plan.min_setup_quality))
        and extension_score <= 1.45
        and target_efficiency_score >= 0.42
    )
    pattern_driven_direction = bool(
        structure_bias not in {"buy", "sell"}
        and direction in {"BUY", "SELL"}
        and (
            first_pullback_ready
            or breakout_retest_ready
            or failed_opposite_move_confirmed
            or entry_confirmation_ready
            or fast_entry_confirmation_ready
            or elite_pattern_rank >= 0.45
            or abs(breakout_score) >= 0.18
            or abs(pullback_score) >= 0.18
            or strong_generic_pattern_ready
        )
    )
    neutral_pattern_pullback_ready = bool(
        structure_bias not in {"buy", "sell"}
        and direction in {"BUY", "SELL"}
        and (
            first_pullback_ready
            or entry_confirmation_ready
            or fast_entry_confirmation_ready
            or pattern_family.endswith("first_pullback")
            or (
                pattern_family.endswith("generic")
                and pattern_family_direction == direction
                and alignment_score >= max(0.58, float(plan.min_alignment_score) - 0.04)
                and setup_quality >= max(0.54, float(plan.min_setup_quality) - 0.04)
                and target_efficiency_score >= 0.36
                and extension_score <= 1.60
            )
            or elite_pattern_rank >= 0.34
        )
    )

    return _SeedState(
        direction=direction,
        direction_source=direction_source,
        structure_bias=structure_bias,
        pattern_family=pattern_family,
        pattern_family_direction=pattern_family_direction,
        alignment_score=alignment_score,
        setup_quality=setup_quality,
        pullback_score=pullback_score,
        breakout_score=breakout_score,
        extension_score=extension_score,
        target_efficiency_score=target_efficiency_score,
        elite_pattern_rank=elite_pattern_rank,
        first_pullback_ready=first_pullback_ready,
        breakout_retest_ready=breakout_retest_ready,
        failed_opposite_move_confirmed=failed_opposite_move_confirmed,
        entry_confirmation_ready=entry_confirmation_ready,
        fast_entry_confirmation_ready=fast_entry_confirmation_ready,
        context_confluence=context_confluence,
        cross_alignment=cross_alignment,
        cross_confidence=cross_confidence,
        micro_score=micro_score,
        whale_context_support=whale_context_support,
        support_components=support_components,
        conflict_components=conflict_components,
        context_pressure_ready=context_pressure_ready,
        context_driven_direction=context_driven_direction,
        strong_generic_pattern_ready=strong_generic_pattern_ready,
        pattern_driven_direction=pattern_driven_direction,
        neutral_pattern_pullback_ready=neutral_pattern_pullback_ready,
    )


def _depth_context_pressure_profile(
    category: str,
    context_confluence: Dict[str, Any],
) -> Dict[str, Any]:
    category_key = str(category or "").strip().lower()
    source = str(context_confluence.get("microstructure_source") or "").strip().lower()
    provider = str(context_confluence.get("depth_provider") or "").strip().lower()
    provider_class = str(context_confluence.get("depth_provider_class") or "").strip().lower()
    environment = str(context_confluence.get("depth_environment") or "").strip().lower()
    depth_levels = int(context_confluence.get("depth_levels", 0) or 0)
    depth_quality = _clip(_safe_float(context_confluence.get("depth_quality"), 0.0), 0.0, 1.0)
    provider_trust = _clip(_safe_float(context_confluence.get("depth_provider_trust_score"), 0.0), 0.0, 1.0)
    quote_alignment = _clip(_safe_float(context_confluence.get("depth_quote_alignment_score"), 0.0), 0.0, 1.0)
    quote_state = str(context_confluence.get("depth_quote_agreement_state") or "").strip().lower()
    update_mode = str(context_confluence.get("depth_update_mode") or "").strip().lower()
    synthetic_depth = bool(context_confluence.get("synthetic_depth"))
    depth_available = bool(context_confluence.get("depth_available"))
    authority_tier = str(context_confluence.get("dom_authority_tier") or "").strip().lower()
    fragmented = bool(context_confluence.get("dom_fragmented_market")) or authority_tier in {
        "fragmented_event_ladder",
        "degraded_event_ladder",
    }
    stream_degraded = bool(context_confluence.get("dom_stream_degraded")) and bool(
        context_confluence.get("dom_ladder_ready")
    )
    exchange_depth = bool(
        provider_class == "exchange_depth"
        or any(token in provider for token in ("binance", "bybit", "okx"))
        or source in {"binance_rest_depth", "binance_live_depth"}
        or (source == "live_store_depth" and provider in {"binance", "bybit", "okx"})
    )
    sidecar_depth = bool(
        any(token in provider for token in ("dukascopy", "ctrader"))
        or source in {"dukascopy_live_depth", "ctrader_live_depth"}
        or (provider_class == "sidecar" and category_key in {"forex", "indices", "commodities"})
    )
    redis_depth = bool(provider_class == "redis_subscriber" or source == "order_flow_true_depth")

    if not depth_available or synthetic_depth:
        return {"ready": False, "reason": "depth_unavailable", "kind": "none"}
    if bool(context_confluence.get("external_depth_rejected")) or quote_state in {"divergent", "severe_divergence"}:
        return {"ready": False, "reason": "depth_quote_divergent", "kind": "none"}

    kind = "exchange" if exchange_depth or redis_depth else "sidecar" if sidecar_depth else "depth"
    if exchange_depth or redis_depth:
        min_levels = 50 if exchange_depth else 8
        min_quality = 0.45
        min_trust = 0.72 if category_key == "crypto" else 0.78
        alignment_floor = 0.30
        setup_floor = 0.46
        target_floor = 0.55 if category_key == "crypto" else 0.50
        extension_ceiling = 1.05 if category_key == "crypto" else 1.12
        flow_floor = 0.30 if category_key == "crypto" else 0.28
        confluence_floor = 0.14
    elif sidecar_depth:
        min_levels = 2
        min_quality = 0.25
        min_trust = 0.58 if "ctrader" in provider and environment not in {"", "live", "real", "production"} else 0.60
        alignment_floor = 0.36 if category_key == "commodities" else 0.40
        setup_floor = 0.48 if category_key == "commodities" else 0.50
        target_floor = 0.48
        extension_ceiling = 1.08
        flow_floor = 0.24 if category_key in {"forex", "indices"} else 0.26
        confluence_floor = 0.15
    else:
        min_levels = 4
        min_quality = 0.35
        min_trust = 0.64
        alignment_floor = 0.42
        setup_floor = 0.50
        target_floor = 0.50
        extension_ceiling = 1.05
        flow_floor = 0.28
        confluence_floor = 0.16

    if fragmented or stream_degraded:
        kind = f"{kind}_degraded"
        if exchange_depth:
            min_levels = max(min_levels, 50)
            min_quality = max(min_quality, 0.74)
            min_trust = max(min_trust, 0.82)
            alignment_floor = max(alignment_floor, 0.36)
            setup_floor = max(setup_floor, 0.50)
            flow_floor = max(flow_floor, 0.42)
            confluence_floor = max(confluence_floor, 0.20)
        elif sidecar_depth:
            min_quality = max(min_quality, 0.36)
            min_trust = max(min_trust, 0.68)
            flow_floor = max(flow_floor, 0.34)
            confluence_floor = max(confluence_floor, 0.18)
        else:
            min_quality = max(min_quality, 0.45)
            min_trust = max(min_trust, 0.72)
            flow_floor = max(flow_floor, 0.36)
            confluence_floor = max(confluence_floor, 0.18)

    if depth_levels < min_levels:
        return {"ready": False, "reason": "depth_levels_too_low", "kind": kind}
    if depth_quality < min_quality:
        return {"ready": False, "reason": "depth_quality_too_low", "kind": kind}
    if provider_trust < min_trust:
        return {"ready": False, "reason": "depth_trust_too_low", "kind": kind}
    if quote_alignment and quote_alignment < 0.75 and quote_state not in {"", "unconfirmed"}:
        return {"ready": False, "reason": "depth_quote_alignment_too_low", "kind": kind}
    if update_mode in {"none", "synthetic", "top_quote"}:
        return {"ready": False, "reason": "depth_update_mode_weak", "kind": kind}

    return {
        "ready": True,
        "reason": "",
        "kind": kind,
        "alignment_floor": alignment_floor,
        "setup_floor": setup_floor,
        "target_floor": target_floor,
        "extension_ceiling": extension_ceiling,
        "flow_floor": flow_floor,
        "confluence_floor": confluence_floor,
        "breakout_ignition_target_floor": max(
            0.08,
            target_floor - (0.44 if exchange_depth or redis_depth else 0.36 if sidecar_depth else 0.30),
        ),
        "breakout_ignition_extension_ceiling": max(
            extension_ceiling,
            1.62 if exchange_depth or redis_depth else 1.48 if sidecar_depth else 1.38,
        ),
        "breakout_ignition_impulse_age_limit": 8 if exchange_depth or redis_depth else 7,
        "breakout_ignition_flow_floor": max(0.22, flow_floor - 0.12),
        "breakout_ignition_confluence_floor": max(0.12, confluence_floor - 0.08),
    }


def _no_seed_probe_reason(
    *,
    category: str,
    structure: Dict[str, Any],
    plan: _AssetPlaybookPlan,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    category_key = str(category or "").strip().lower()
    if category_key not in {"crypto", "commodities", "forex", "indices"}:
        return "no_playbook_builder_ready"

    seed_state = _build_seed_state(structure=structure, plan=plan, context=context)
    direction = seed_state.direction
    if direction not in {"BUY", "SELL"}:
        return "depth_context_pressure_wait:no_direction"
    if seed_state.structure_bias not in {"buy", "sell"}:
        return "depth_context_pressure_wait:neutral_structure"
    if not (seed_state.pattern_family.startswith("trending_") and seed_state.pattern_family.endswith("generic")):
        return "depth_context_pressure_wait:pattern_not_generic_trend"
    if _pattern_family_direction(seed_state.pattern_family) != direction:
        return "depth_context_pressure_wait:family_direction_mismatch"

    context_confluence = _context_directional_confluence(context, direction)
    depth_profile = _depth_context_pressure_profile(category_key, context_confluence)
    if not depth_profile.get("ready"):
        return f"depth_context_pressure_wait:{depth_profile.get('reason') or 'depth_unavailable'}"
    if int(context_confluence.get("support_components", 0) or 0) < 1:
        return "depth_context_pressure_wait:context_support_missing"
    if int(context_confluence.get("conflict_components", 0) or 0) > 0:
        return "depth_context_pressure_wait:context_conflict"
    if _safe_float(structure.get("alignment_score"), 0.0) < _safe_float(depth_profile.get("alignment_floor"), 0.42):
        return "depth_context_pressure_wait:alignment_too_weak"
    if _safe_float(structure.get("setup_quality"), 0.0) < _safe_float(depth_profile.get("setup_floor"), 0.50):
        return "depth_context_pressure_wait:setup_quality_too_weak"
    breakout_ignition_probe = bool(
        _safe_float(structure.get("target_efficiency_score"), 0.0)
        >= _safe_float(depth_profile.get("breakout_ignition_target_floor"), 0.08)
        and _safe_float(structure.get("extension_score"), 0.0)
        <= _safe_float(depth_profile.get("breakout_ignition_extension_ceiling"), 1.38)
        and int(structure.get("impulse_age_bars", 0) or 0)
        <= int(depth_profile.get("breakout_ignition_impulse_age_limit", 7) or 7)
        and _safe_float(structure.get("cluster_penalty"), 0.0) <= 0.20
        and (
            _safe_float(structure.get("breakout_score"), 0.0)
            * (1.0 if direction == "BUY" else -1.0)
            >= 0.06
            or _safe_float(context_confluence.get("micro_support"), 0.0)
            >= _safe_float(depth_profile.get("breakout_ignition_flow_floor"), 0.22)
        )
        and _safe_float(context_confluence.get("score"), 0.0)
        >= _safe_float(depth_profile.get("breakout_ignition_confluence_floor"), 0.12)
    )
    if (
        _safe_float(structure.get("target_efficiency_score"), 0.0) < _safe_float(depth_profile.get("target_floor"), 0.50)
        and not breakout_ignition_probe
    ):
        return "depth_context_pressure_wait:target_space_too_thin"
    if (
        _safe_float(structure.get("extension_score"), 0.0) > _safe_float(depth_profile.get("extension_ceiling"), 1.05)
        and not breakout_ignition_probe
    ):
        return "depth_context_pressure_wait:entry_extended"
    if int(structure.get("impulse_age_bars", 0) or 0) > 6 and not breakout_ignition_probe:
        return "depth_context_pressure_wait:setup_too_old"
    if _safe_float(structure.get("cluster_penalty"), 0.0) > 0.18 and not breakout_ignition_probe:
        return "depth_context_pressure_wait:cluster_risk"
    if (
        max(
            abs(_safe_float(context_confluence.get("micro_support"), 0.0)),
            abs(_safe_float(context_confluence.get("cross_support"), 0.0)),
            abs(_safe_float(context_confluence.get("whale_support"), 0.0)),
        )
        < _safe_float(depth_profile.get("flow_floor"), 0.28)
    ):
        return "depth_context_pressure_wait:flow_too_weak"
    if _safe_float(context_confluence.get("score"), 0.0) < _safe_float(depth_profile.get("confluence_floor"), 0.16):
        return "depth_context_pressure_wait:context_too_weak"
    return "depth_context_pressure_wait:no_builder_selected"


_CATEGORY_PROFILES: Dict[str, _PlaybookProfile] = {
    "forex": _PlaybookProfile(0.56, 0.58, 0.57, 0.57, 0.58, 0.58, 0.52, 0.66, 0.12, 0.32, 18, "5m", ("europe", "us"), 3, 0.18, 2.25, 1.15, 0.82),
    "crypto": _PlaybookProfile(0.58, 0.60, 0.58, 0.59, 0.60, 0.60, 0.54, 0.68, 0.10, 0.36, 20, "5m", ("asia", "europe", "us"), 4, 0.25, 2.8, 1.15, 1.20),
    "commodities": _PlaybookProfile(0.57, 0.58, 0.57, 0.58, 0.58, 0.59, 0.53, 0.67, 0.11, 0.34, 18, "5m", ("europe", "us"), 3, 0.22, 2.4, 1.20, 1.02),
    "indices": _PlaybookProfile(0.57, 0.59, 0.57, 0.58, 0.58, 0.59, 0.53, 0.67, 0.11, 0.34, 18, "5m", ("us",), 3, 0.20, 2.2, 1.15, 0.96),
}

_TREND_PLAYBOOKS = {
    "breakout_continuation",
    "breakout_retest",
    "trend_pullback",
    "aggressive_expansion",
    "intermarket_continuation",
    "opening_drive",
    "news_impulse",
    "crypto_orderflow_continuation",
}

_REVERSAL_PLAYBOOKS = {
    "reversal_exhaustion",
    "failed_break_reclaim",
}

_EARLY_INFLECTION_PLAYBOOKS = {
    "early_inflection",
}

_PREMIUM_ENTRY_PLAYBOOKS = {
    "breakout_retest",
    "trend_pullback",
    "failed_break_reclaim",
}

_FAST_ENTRY_PLAYBOOKS = {
    "breakout_continuation",
    "aggressive_expansion",
    "intermarket_continuation",
    "opening_drive",
    "news_impulse",
    "crypto_orderflow_continuation",
}

_CATEGORY_PLANS: Dict[str, _AssetPlaybookPlan] = {
    "forex": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("europe", "us"),
        0.56,
        0.54,
        1,
        1,
    ),
    "crypto": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("europe", "us"),
        0.58,
        0.56,
        1,
        1,
    ),
    "commodities": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("europe", "us"),
        0.57,
        0.55,
        1,
        1,
    ),
    "indices": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("us",),
        0.58,
        0.56,
        1,
        1,
    ),
}

_ASSET_PLANS: Dict[str, _AssetPlaybookPlan] = {
    "EUR/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "GBP/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "USD/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "EUR/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.61,
        0.59,
        2,
        1,
    ),
    "EUR/GBP": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "GBP/JPY": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.62,
        0.60,
        2,
        1,
    ),
    "AUD/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "NZD/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "USD/CAD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "USD/CHF": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "news_impulse"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "XAU/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.58,
        0.57,
        1,
        1,
    ),
    "XAG/USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "aggressive_expansion", "intermarket_continuation", "opening_drive", "news_impulse"),
        ("europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.59,
        0.58,
        1,
        1,
    ),
    "WTI": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "aggressive_expansion", "intermarket_continuation", "opening_drive", "news_impulse"),
        ("us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        2,
        1,
    ),
    "US30": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("us_overlap", "us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "US100": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("us_overlap", "us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "US500": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("us_overlap", "us_open", "us_core"),
        0.59,
        0.57,
        2,
        1,
    ),
    "UK100": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("europe_open", "europe_core", "us_overlap"),
        0.58,
        0.56,
        2,
        1,
    ),
    "GER40": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("europe_open", "europe_core", "us_overlap"),
        0.58,
        0.56,
        2,
        1,
    ),
    "AUS200": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("asia_core", "europe_open"),
        0.58,
        0.56,
        2,
        1,
    ),
    "JPN225": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "intermarket_continuation", "opening_drive"),
        ("asia_core", "europe_open"),
        0.58,
        0.56,
        2,
        1,
    ),
    "BTC-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "early_inflection", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        1,
        1,
    ),
    "ETH-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.60,
        0.58,
        1,
        1,
    ),
    "BNB-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.62,
        0.60,
        2,
        1,
    ),
    "SOL-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.62,
        0.60,
        2,
        1,
    ),
    "XRP-USD": _AssetPlaybookPlan(
        ("breakout_continuation", "breakout_retest", "trend_pullback", "failed_break_reclaim", "reversal_exhaustion", "aggressive_expansion", "crypto_orderflow_continuation"),
        ("asia_core", "europe_open", "europe_core", "us_overlap", "us_open", "us_core"),
        0.63,
        0.61,
        2,
        1,
    ),
}

_ASSET_MANAGEMENT_OVERRIDES: Dict[str, Dict[str, Any]] = {
    "EUR/USD": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.78},
    "EUR/GBP": {"preferred_interval": "5m", "runner_target_rr": 2.1, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.76},
    "GBP/USD": {"preferred_interval": "5m", "runner_target_rr": 2.3, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.82},
    "USD/JPY": {"preferred_interval": "5m", "runner_target_rr": 2.35, "trail_activation_rr": 1.20, "trail_atr_multiple": 0.84},
    "USD/CHF": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.80},
    "EUR/JPY": {"preferred_interval": "5m", "runner_target_rr": 2.35, "trail_activation_rr": 1.20, "trail_atr_multiple": 0.88},
    "GBP/JPY": {"preferred_interval": "5m", "runner_target_rr": 2.45, "trail_activation_rr": 1.20, "trail_atr_multiple": 0.90},
    "AUD/USD": {"preferred_interval": "15m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.80},
    "NZD/USD": {"preferred_interval": "15m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.80},
    "USD/CAD": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.82},
    "XAU/USD": {"preferred_interval": "5m", "runner_target_rr": 2.6, "trail_activation_rr": 1.20, "trail_atr_multiple": 1.00},
    "XAG/USD": {"preferred_interval": "5m", "runner_target_rr": 2.8, "trail_activation_rr": 1.25, "trail_atr_multiple": 1.08},
    "WTI": {"preferred_interval": "15m", "runner_target_rr": 2.9, "trail_activation_rr": 1.25, "trail_atr_multiple": 1.15},
    "US30": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.92},
    "US100": {"preferred_interval": "5m", "runner_target_rr": 2.35, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.96},
    "US500": {"preferred_interval": "5m", "runner_target_rr": 2.15, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.90},
    "UK100": {"preferred_interval": "5m", "runner_target_rr": 2.15, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.90},
    "GER40": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.92},
    "AUS200": {"preferred_interval": "5m", "runner_target_rr": 2.15, "trail_activation_rr": 1.10, "trail_atr_multiple": 0.90},
    "JPN225": {"preferred_interval": "5m", "runner_target_rr": 2.2, "trail_activation_rr": 1.15, "trail_atr_multiple": 0.94},
    "BTC-USD": {"preferred_interval": "5m", "runner_target_rr": 2.8, "trail_activation_rr": 1.1, "trail_atr_multiple": 1.15},
    "ETH-USD": {"preferred_interval": "5m", "runner_target_rr": 2.7, "trail_activation_rr": 1.05, "trail_atr_multiple": 1.12},
    "BNB-USD": {"preferred_interval": "15m", "runner_target_rr": 3.0, "trail_activation_rr": 1.15, "trail_atr_multiple": 1.18},
    "SOL-USD": {"preferred_interval": "15m", "runner_target_rr": 3.1, "trail_activation_rr": 1.15, "trail_atr_multiple": 1.20},
    "XRP-USD": {"preferred_interval": "15m", "runner_target_rr": 3.2, "trail_activation_rr": 1.2, "trail_atr_multiple": 1.25},
}


class PlaybookService:
    def _profile(self, category: str) -> _PlaybookProfile:
        return _CATEGORY_PROFILES.get(str(category or "").strip().lower(), _CATEGORY_PROFILES["forex"])

    def _asset_plan(self, asset: str, category: str) -> _AssetPlaybookPlan:
        canonical = str(asset or "").strip().upper()
        return _ASSET_PLANS.get(canonical, _CATEGORY_PLANS.get(str(category or "").strip().lower(), _CATEGORY_PLANS["forex"]))

    @staticmethod
    def _frame(price_data) -> Optional[pd.DataFrame]:
        if price_data is None or getattr(price_data, "empty", True):
            return None
        frame = price_data.copy()
        frame.columns = [str(c).lower() for c in frame.columns]
        required = {"open", "high", "low", "close"}
        if not required.issubset(set(frame.columns)) or len(frame) < 25:
            return None
        try:
            for col in required:
                frame[col] = frame[col].astype(float)
        except Exception:
            return None
        return frame

    @staticmethod
    def _atr(frame: pd.DataFrame, period: int = 14) -> float:
        if frame is None or len(frame) < period + 1:
            return 0.0
        high = frame["high"]
        low = frame["low"]
        close = frame["close"]
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        try:
            return float(tr.tail(period).mean())
        except Exception:
            return 0.0

    def _management_template(
        self,
        profile: _PlaybookProfile,
        playbook: str,
        *,
        asset: str,
        category: str,
    ) -> Dict[str, Any]:
        canonical = str(asset or "").strip().upper()
        overrides = dict(_ASSET_MANAGEMENT_OVERRIDES.get(canonical, {}))
        preferred_interval = str(overrides.get("preferred_interval") or profile.preferred_interval or "").strip().lower()
        runner_target_rr = _safe_float(overrides.get("runner_target_rr", profile.runner_target_rr), profile.runner_target_rr)
        trail_activation_rr = _safe_float(overrides.get("trail_activation_rr", profile.trail_activation_rr), profile.trail_activation_rr)
        trail_atr_multiple = _safe_float(overrides.get("trail_atr_multiple", profile.trail_atr_multiple), profile.trail_atr_multiple)
        partial_take_profit_rr = {
            "forex": [1.15, 1.75],
            "commodities": [1.20, 1.90],
            "indices": [1.15, 1.80],
            "crypto": [1.30, 2.10],
        }.get(str(category or "").strip().lower(), [1.15, 1.75])
        partial_take_profit_size_fractions = [0.30, 0.40, 0.30]

        if playbook in _REVERSAL_PLAYBOOKS:
            runner_target_rr = max(1.6, runner_target_rr * 0.88)
            trail_activation_rr = min(trail_activation_rr, 1.0)
            partial_take_profit_rr = [1.0, 1.55]
            partial_take_profit_size_fractions = [0.40, 0.35, 0.25]
        elif playbook == "early_inflection":
            runner_target_rr = max(1.5, runner_target_rr * 0.80)
            trail_activation_rr = min(trail_activation_rr, 0.95)
            trail_atr_multiple = min(trail_atr_multiple, 0.95)
            partial_take_profit_rr = [1.0, 1.45]
            partial_take_profit_size_fractions = [0.40, 0.35, 0.25]
        elif playbook == "opening_drive":
            runner_target_rr = max(1.7, runner_target_rr * 0.92)
            trail_activation_rr = min(trail_activation_rr, 1.05)
            partial_take_profit_rr = [1.2, 1.9]
        elif playbook == "news_impulse":
            runner_target_rr = max(1.8, runner_target_rr * 0.95)
            trail_activation_rr = min(trail_activation_rr, 1.05)
            partial_take_profit_rr = [1.25, 2.0]
        elif playbook == "intermarket_continuation":
            runner_target_rr = max(1.9, runner_target_rr * 0.98)
            trail_activation_rr = min(trail_activation_rr, 1.05)
            partial_take_profit_rr = [1.25, 2.0]
        elif playbook == "crypto_orderflow_continuation":
            runner_target_rr = max(runner_target_rr, 2.6)
            trail_atr_multiple = max(trail_atr_multiple, 1.05)
            partial_take_profit_rr = [1.4, 2.25]

        return {
            "style": "intraday_playbook",
            "playbook": playbook,
            "asset": canonical,
            "category": str(category or "").strip().lower(),
            "partial_take_profit_rr": partial_take_profit_rr,
            "partial_take_profit_size_fractions": partial_take_profit_size_fractions,
            "runner_target_rr": round(float(runner_target_rr), 4),
            "trail_activation_rr": round(float(trail_activation_rr), 4),
            "trail_atr_multiple": round(float(trail_atr_multiple), 4),
            "trail_mode": "extreme_atr",
            "break_even_after_partial": True,
            "preferred_interval": preferred_interval,
        }

    def preferred_interval(self, category: str, asset: str = "") -> str:
        canonical = str(asset or "").strip().upper()
        override = _ASSET_MANAGEMENT_OVERRIDES.get(canonical, {})
        interval = str(override.get("preferred_interval", "") or "").strip().lower()
        if interval:
            return interval
        return self._profile(category).preferred_interval

    @staticmethod
    def _trend_sign(state: str) -> int:
        label = str(state or "").strip().lower()
        if label == "trending_up":
            return 1
        if label == "trending_down":
            return -1
        return 0

    @staticmethod
    def _default_allowed_sessions(
        asset: str,
        category: str,
        profile: _PlaybookProfile,
        plan: _AssetPlaybookPlan,
    ) -> tuple[str, ...]:
        if plan.allowed_sessions:
            return plan.allowed_sessions
        canonical = str(asset or "").strip().upper()
        if str(category or "").strip().lower() == "indices":
            if canonical == "UK100":
                return ("europe",)
            return ("us",)
        return profile.allowed_sessions

    def _session_allowed(self, asset: str, category: str) -> tuple[bool, str, tuple[str, ...]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        current = _active_session(category=category)
        allowed = self._default_allowed_sessions(asset, category, profile, plan)
        if not allowed:
            return True, current, allowed
        return any(_session_matches(current, item) for item in allowed), current, allowed

    def _qualify_candidate(
        self,
        candidate: Dict[str, Any],
        *,
        asset: str,
        category: str,
        structure: Dict[str, Any],
        plan: _AssetPlaybookPlan,
        inactivity_profile: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> tuple[bool, str]:
        playbook = str(candidate.get("playbook") or "").strip()
        if playbook not in plan.allowed_playbooks:
            return False, f"playbook_not_allowed:{playbook}"

        direction = str(candidate.get("direction") or "").upper()
        direction_sign = _playbook_direction_sign(direction)
        if direction_sign == 0:
            return False, f"invalid_direction:{playbook}"

        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        upside_exhaustion_score = float(structure.get("upside_exhaustion_score", 0.0) or 0.0)
        downside_exhaustion_score = float(structure.get("downside_exhaustion_score", 0.0) or 0.0)
        trend_5m = str(structure.get("trend_5m", "unknown") or "unknown").lower()
        trend_15m = str(structure.get("trend_15m", "unknown") or "unknown").lower()
        trend_1h = str(structure.get("trend_1h", "unknown") or "unknown").lower()
        pattern_family = str(structure.get("pattern_family", "unknown") or "unknown").lower()
        entry_confirmation_ready = bool(structure.get("entry_confirmation_ready"))
        entry_confirmation_count = int(structure.get("entry_confirmation_count", 0) or 0)
        entry_confirmation_bars_required = int(structure.get("entry_confirmation_bars_required", 0) or 0)
        fast_entry_confirmation_ready = bool(structure.get("fast_entry_confirmation_ready"))
        fast_entry_confirmation_count = int(structure.get("fast_entry_confirmation_count", 0) or 0)
        fast_entry_confirmation_bars_required = int(structure.get("fast_entry_confirmation_bars_required", 0) or 0)
        target_efficiency_score = float(structure.get("target_efficiency_score", 0.0) or 0.0)
        extension_score = float(structure.get("extension_score", 0.0) or 0.0)
        impulse_age_bars = int(structure.get("impulse_age_bars", 0) or 0)
        elite_pattern_rank = float(structure.get("elite_pattern_rank", 0.0) or 0.0)
        cluster_penalty = float(structure.get("cluster_penalty", 0.0) or 0.0)
        has_execution_structure = any(
            key in structure
            for key in (
                "target_efficiency_score",
                "extension_score",
                "impulse_age_bars",
                "elite_pattern_rank",
                "cluster_penalty",
                "entry_confirmation_bars_required",
                "fast_entry_confirmation_bars_required",
            )
        )
        trigger_trend_aligned = bool(structure.get("trigger_trend_aligned"))
        structure_promoted = bool(structure.get("structure_promoted"))
        external_confirmation_score = float(structure.get("external_confirmation_score", 0.0) or 0.0)
        liquidity_sweep_buy = bool(structure.get("liquidity_sweep_buy"))
        liquidity_sweep_sell = bool(structure.get("liquidity_sweep_sell"))
        preferred_interval = str(candidate.get("preferred_interval") or self.preferred_interval(category, asset) or "").strip().lower()

        trend_states = (trend_15m, trend_1h)
        aligned_trends = sum(1 for state in trend_states if self._trend_sign(state) == direction_sign)
        opposing_trends = sum(1 for state in trend_states if self._trend_sign(state) == -direction_sign)
        bias_alignment = (
            (structure_bias == "buy" and direction == "BUY")
            or (structure_bias == "sell" and direction == "SELL")
        )
        effective_confirmation_ready, effective_confirmation_count, effective_confirmation_required, fast_confirmation_override = _effective_confirmation_gate(
            playbook=playbook,
            entry_confirmation_ready=entry_confirmation_ready,
            entry_confirmation_count=entry_confirmation_count,
            entry_confirmation_bars_required=entry_confirmation_bars_required,
            fast_entry_confirmation_ready=fast_entry_confirmation_ready,
            fast_entry_confirmation_count=fast_entry_confirmation_count,
            fast_entry_confirmation_bars_required=fast_entry_confirmation_bars_required,
        )
        strong_impulse_break = False
        allow_early_trend_relief = False
        inactivity_profile = inactivity_profile if isinstance(inactivity_profile, dict) else {}
        inactivity_relief_strength = _safe_float(inactivity_profile.get("relief_strength"), 0.0)
        inactivity_flat_book = bool(inactivity_profile.get("flat_book")) or bool(inactivity_profile.get("equity_relief"))
        liquidity_sweep_directional = (
            (direction == "BUY" and liquidity_sweep_buy)
            or (direction == "SELL" and liquidity_sweep_sell)
        )
        context_confluence = _context_directional_confluence(context, direction)
        context_score = _safe_float(context_confluence.get("score"), 0.0)
        context_cross_support = _safe_float(context_confluence.get("cross_support"), 0.0)
        context_micro_support = _safe_float(context_confluence.get("micro_support"), 0.0)
        context_whale_support = _safe_float(context_confluence.get("whale_support"), 0.0)
        context_support_count = int(context_confluence.get("support_components", 0) or 0)
        context_conflict_count = int(context_confluence.get("conflict_components", 0) or 0)
        depth_context_ready = bool(_depth_context_pressure_profile(category, context_confluence).get("ready"))
        shock_profile = _shared_shock_profile(
            candidate=candidate,
            structure=structure,
            context=context,
            direction=direction,
            category=category,
        )
        candidate["shock_score"] = float(shock_profile.get("score", 0.0) or 0.0)
        candidate["shock_event_score"] = float(shock_profile.get("event_score", 0.0) or 0.0)
        candidate["headline_shock_score"] = float(shock_profile.get("headline_shock_score", 0.0) or 0.0)
        candidate["headline_shock_raw_score"] = float(shock_profile.get("headline_shock_raw_score", 0.0) or 0.0)
        candidate["headline_shock_direction"] = str(shock_profile.get("headline_shock_direction") or "")
        candidate["headline_shock_directional_score"] = float(shock_profile.get("headline_shock_directional_score", 0.0) or 0.0)
        candidate["headline_shock_direction_weight"] = float(shock_profile.get("headline_shock_direction_weight", 0.0) or 0.0)
        candidate["shock_displacement_score"] = float(shock_profile.get("displacement_score", 0.0) or 0.0)
        candidate["shock_structure_score"] = float(shock_profile.get("structure_score", 0.0) or 0.0)
        candidate["shock_liquidity_score"] = float(shock_profile.get("liquidity_score", 0.0) or 0.0)
        candidate["shock_timing_score"] = float(shock_profile.get("timing_score", 0.0) or 0.0)
        candidate["shock_fresh_event"] = bool(shock_profile.get("fresh_event"))
        candidate["shock_supported"] = bool(shock_profile.get("supported"))
        candidate["shock_event_label"] = str(shock_profile.get("event_label") or "")
        qualification: Dict[str, Any] = {
            "playbook": playbook,
            "direction": direction,
            "score": round(float(candidate.get("score", 0.0) or 0.0), 4),
            "confidence": round(float(candidate.get("confidence", 0.0) or 0.0), 4),
            "pattern_family": pattern_family,
            "trend_5m": trend_5m,
            "aligned_trends": aligned_trends,
            "opposing_trends": opposing_trends,
            "required_trends": int(plan.min_trend_agreement or 0),
            "alignment_score": round(alignment_score, 4),
            "setup_quality": round(setup_quality, 4),
            "entry_confirmation_ready": bool(entry_confirmation_ready),
            "entry_confirmation_count": int(entry_confirmation_count),
            "entry_confirmation_bars_required": int(entry_confirmation_bars_required),
            "target_efficiency_score": round(target_efficiency_score, 4),
            "extension_score": round(extension_score, 4),
            "impulse_age_bars": int(impulse_age_bars),
            "elite_pattern_rank": round(elite_pattern_rank, 4),
            "cluster_penalty": round(cluster_penalty, 4),
            "effective_confirmation_ready": bool(effective_confirmation_ready),
            "effective_confirmation_count": int(effective_confirmation_count),
            "effective_confirmation_bars_required": int(effective_confirmation_required),
            "fast_confirmation_override": bool(fast_confirmation_override),
            "trigger_trend_aligned": bool(trigger_trend_aligned),
            "structure_promoted": bool(structure_promoted),
            "external_confirmation_score": round(external_confirmation_score, 4),
            "preferred_interval": preferred_interval,
            "liquidity_sweep_directional": bool(liquidity_sweep_directional),
            "inactivity_relief_strength": round(inactivity_relief_strength, 4),
            "inactivity_flat_book": inactivity_flat_book,
            "context_confluence": round(context_score, 4),
            "cross_context_support": round(context_cross_support, 4),
            "micro_context_support": round(context_micro_support, 4),
            "whale_context_support": round(context_whale_support, 4),
            "support_components": context_support_count,
            "conflict_components": context_conflict_count,
            "shock_score": round(float(shock_profile.get("score", 0.0) or 0.0), 4),
            "shock_event_score": round(float(shock_profile.get("event_score", 0.0) or 0.0), 4),
            "headline_shock_score": round(float(shock_profile.get("headline_shock_score", 0.0) or 0.0), 4),
            "headline_shock_raw_score": round(float(shock_profile.get("headline_shock_raw_score", 0.0) or 0.0), 4),
            "headline_shock_direction": str(shock_profile.get("headline_shock_direction") or ""),
            "headline_shock_directional_score": round(float(shock_profile.get("headline_shock_directional_score", 0.0) or 0.0), 4),
            "headline_shock_direction_weight": round(float(shock_profile.get("headline_shock_direction_weight", 0.0) or 0.0), 4),
            "shock_displacement_score": round(float(shock_profile.get("displacement_score", 0.0) or 0.0), 4),
            "shock_structure_score": round(float(shock_profile.get("structure_score", 0.0) or 0.0), 4),
            "shock_liquidity_score": round(float(shock_profile.get("liquidity_score", 0.0) or 0.0), 4),
            "shock_timing_score": round(float(shock_profile.get("timing_score", 0.0) or 0.0), 4),
            "shock_fresh_event": bool(shock_profile.get("fresh_event")),
            "shock_supported": bool(shock_profile.get("supported")),
            "shock_event_label": str(shock_profile.get("event_label") or ""),
        }

        if playbook == "crypto_orderflow_continuation":
            ok, reason, _ = _qualify_crypto_orderflow_candidate(
                candidate=candidate,
                profile=self._profile(category),
                plan=plan,
                playbook=playbook,
                direction=direction,
                structure_bias=structure_bias,
                alignment_score=alignment_score,
                setup_quality=setup_quality,
                upside_exhaustion_score=upside_exhaustion_score,
                downside_exhaustion_score=downside_exhaustion_score,
                aligned_trends=aligned_trends,
                bias_alignment=bias_alignment,
            )
            if not ok:
                qualification["reason"] = reason
                candidate["qualification"] = qualification
                return False, reason
        elif playbook in {"aggressive_expansion", "breakout_continuation", "news_impulse", "intermarket_continuation", "opening_drive"}:
            ok, reason, strong_impulse_break, allow_early_trend_relief = _qualify_impulse_candidate(
                candidate=candidate,
                profile=self._profile(category),
                plan=plan,
                playbook=playbook,
                direction=direction,
                structure_bias=structure_bias,
                alignment_score=alignment_score,
                setup_quality=setup_quality,
                upside_exhaustion_score=upside_exhaustion_score,
                downside_exhaustion_score=downside_exhaustion_score,
                aligned_trends=aligned_trends,
                bias_alignment=bias_alignment,
                entry_style=str(candidate.get("entry_style") or ""),
                pattern_family=pattern_family,
                entry_confirmation_ready=effective_confirmation_ready,
                entry_confirmation_count=effective_confirmation_count,
                entry_confirmation_bars_required=effective_confirmation_required,
                target_efficiency_score=target_efficiency_score,
                extension_score=extension_score,
                impulse_age_bars=impulse_age_bars,
                elite_pattern_rank=elite_pattern_rank,
                cluster_penalty=cluster_penalty,
                has_execution_structure=has_execution_structure,
                liquidity_sweep_directional=liquidity_sweep_directional,
                preferred_interval=preferred_interval,
                trigger_trend_aligned=trigger_trend_aligned,
                structure_promoted=structure_promoted,
                external_confirmation_score=external_confirmation_score,
                inactivity_relief_strength=inactivity_relief_strength,
                inactivity_flat_book=inactivity_flat_book,
                shock_score=float(shock_profile.get("score", 0.0) or 0.0),
                shock_event_score=float(shock_profile.get("event_score", 0.0) or 0.0),
                shock_displacement_score=float(shock_profile.get("displacement_score", 0.0) or 0.0),
                shock_structure_score=float(shock_profile.get("structure_score", 0.0) or 0.0),
                shock_liquidity_score=float(shock_profile.get("liquidity_score", 0.0) or 0.0),
                shock_timing_score=float(shock_profile.get("timing_score", 0.0) or 0.0),
                shock_fresh_event=bool(shock_profile.get("fresh_event")),
                shock_supported=bool(shock_profile.get("supported")),
                depth_context_ready=depth_context_ready,
                context_confluence_score=context_score,
                context_support_components=context_support_count,
                context_conflict_components=context_conflict_count,
                context_micro_support=context_micro_support,
                context_cross_support=context_cross_support,
                context_whale_support=context_whale_support,
            )
            qualification["strong_impulse_break"] = bool(strong_impulse_break)
            qualification["allow_early_trend_relief"] = bool(allow_early_trend_relief)
            qualification["effective_required_trends"] = min(int(plan.min_trend_agreement or 0), 1) if allow_early_trend_relief else int(plan.min_trend_agreement or 0)
            if not ok:
                qualification["reason"] = reason
                candidate["qualification"] = qualification
                return False, reason
        else:
            ok, reason = _qualify_standard_candidate(
                playbook=playbook,
                plan=plan,
                alignment_score=alignment_score,
                setup_quality=setup_quality,
                inactivity_relief_strength=inactivity_relief_strength,
                inactivity_flat_book=inactivity_flat_book,
                context_confluence=_safe_float(context_confluence.get("score"), 0.0),
            )
            if not ok:
                qualification["reason"] = reason
                candidate["qualification"] = qualification
                return False, reason

        if (
            playbook == "opening_drive"
            and preferred_interval in {"1m", "5m", "15m"}
            and bias_alignment
            and trigger_trend_aligned
            and alignment_score >= max(0.52, float(plan.min_alignment_score) - 0.08)
            and setup_quality >= max(0.50, float(plan.min_setup_quality) - 0.08)
            and (
                effective_confirmation_ready
                or external_confirmation_score >= 0.16
                or structure_promoted
            )
        ):
            allow_early_trend_relief = True

        reason = _qualify_family_rules(
            playbook=playbook,
            plan=plan,
            structure_bias=structure_bias,
            bias_alignment=bias_alignment,
            aligned_trends=aligned_trends,
            opposing_trends=opposing_trends,
            upside_exhaustion_score=upside_exhaustion_score,
            downside_exhaustion_score=downside_exhaustion_score,
            strong_impulse_break=strong_impulse_break,
            direction=direction,
            allow_early_trend_relief=allow_early_trend_relief,
            reversal_context_support=_safe_float(context_confluence.get("score"), 0.0),
            reversal_support_components=int(context_confluence.get("support_components", 0) or 0),
            reversal_candidate_score=float(candidate.get("score", 0.0) or 0.0),
        )
        if reason:
            qualification["reason"] = reason
            qualification["effective_required_trends"] = min(int(plan.min_trend_agreement or 0), 1) if allow_early_trend_relief else int(plan.min_trend_agreement or 0)
            qualification["strong_impulse_break"] = bool(strong_impulse_break)
            qualification["allow_early_trend_relief"] = bool(allow_early_trend_relief)
            candidate["qualification"] = qualification
            return False, reason

        elite_gate_reason = _elite_entry_gate_reason(playbook=playbook, structure=structure, candidate=candidate)
        if elite_gate_reason:
            qualification["reason"] = elite_gate_reason
            qualification["effective_required_trends"] = min(int(plan.min_trend_agreement or 0), 1) if allow_early_trend_relief else int(plan.min_trend_agreement or 0)
            qualification["strong_impulse_break"] = bool(strong_impulse_break)
            qualification["allow_early_trend_relief"] = bool(allow_early_trend_relief)
            candidate["qualification"] = qualification
            return False, elite_gate_reason

        candidate["context_confluence"] = round(_safe_float(context_confluence.get("score"), 0.0), 4)
        candidate["cross_context_support"] = round(_safe_float(context_confluence.get("cross_support"), 0.0), 4)
        candidate["micro_context_support"] = round(_safe_float(context_confluence.get("micro_support"), 0.0), 4)
        candidate["whale_context_support"] = round(_safe_float(context_confluence.get("whale_support"), 0.0), 4)
        candidate["cross_confidence"] = round(_safe_float(context_confluence.get("cross_confidence"), 0.0), 4)
        candidate["support_components"] = int(context_confluence.get("support_components", 0) or 0)
        candidate["conflict_components"] = int(context_confluence.get("conflict_components", 0) or 0)
        if "cross_alignment" not in candidate:
            candidate["cross_alignment"] = round(_safe_float(context_confluence.get("cross_support"), 0.0), 4)
        if "micro_score" not in candidate:
            candidate["micro_score"] = round(_safe_float(context_confluence.get("micro_support"), 0.0), 4)
        if abs(_safe_float(context_confluence.get("score"), 0.0)) >= 0.08:
            notes = [str(note) for note in list(candidate.get("notes") or [])]
            ctx_note = (
                f"ctx={_safe_float(context_confluence.get('score'), 0.0):+0.2f}"
                f"/micro={_safe_float(context_confluence.get('micro_support'), 0.0):+0.2f}"
                f"/cross={_safe_float(context_confluence.get('cross_support'), 0.0):+0.2f}"
            )
            if ctx_note not in notes:
                notes.append(ctx_note)
            candidate["notes"] = notes

        candidate["asset_plan"] = {
            "allowed_playbooks": list(plan.allowed_playbooks),
            "allowed_sessions": list(plan.allowed_sessions),
            "min_alignment_score": round(float(plan.min_alignment_score), 4),
            "min_setup_quality": round(float(plan.min_setup_quality), 4),
            "min_trend_agreement": int(plan.min_trend_agreement),
        }
        candidate["htf_alignment"] = {
            "trend_15m": trend_15m,
            "trend_1h": trend_1h,
            "structure_bias": structure_bias,
            "aligned_trends": aligned_trends,
            "opposing_trends": opposing_trends,
        }
        qualification["effective_required_trends"] = min(int(plan.min_trend_agreement or 0), 1) if allow_early_trend_relief else int(plan.min_trend_agreement or 0)
        qualification["strong_impulse_break"] = bool(strong_impulse_break)
        qualification["allow_early_trend_relief"] = bool(allow_early_trend_relief)
        qualification["reason"] = ""
        candidate["qualification"] = qualification
        return True, ""

    def _elite_ready_fallback(
        self,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        plan: _AssetPlaybookPlan,
        inactivity_profile: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        seed_state = _build_seed_state(structure=structure, plan=plan, context=context)
        structure_bias = seed_state.structure_bias
        direction = seed_state.direction
        context_driven_direction = seed_state.context_driven_direction
        if not direction:
            return None

        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        candle_quality_score = float(structure.get("candle_quality_score", 0.0) or 0.0)
        session_quality_score = float(structure.get("session_quality_score", 0.0) or 0.0)
        extension_score = float(structure.get("extension_score", 0.0) or 0.0)
        target_efficiency_score = float(structure.get("target_efficiency_score", 0.0) or 0.0)
        impulse_age_bars = int(structure.get("impulse_age_bars", 0) or 0)
        elite_pattern_rank = float(structure.get("elite_pattern_rank", 0.0) or 0.0)
        cluster_penalty = float(structure.get("cluster_penalty", 0.0) or 0.0)
        breakout_retest_ready = bool(structure.get("breakout_retest_ready"))
        first_pullback_ready = bool(structure.get("first_pullback_ready"))
        failed_opposite_move_confirmed = bool(structure.get("failed_opposite_move_confirmed"))
        entry_confirmation_ready = bool(structure.get("entry_confirmation_ready"))
        entry_confirmation_bars_required = int(structure.get("entry_confirmation_bars_required", 0) or 0)
        entry_confirmation_count = int(structure.get("entry_confirmation_count", 0) or 0)
        fast_entry_confirmation_ready = bool(structure.get("fast_entry_confirmation_ready"))
        fast_entry_confirmation_bars_required = int(structure.get("fast_entry_confirmation_bars_required", 0) or 0)
        fast_entry_confirmation_count = int(structure.get("fast_entry_confirmation_count", 0) or 0)
        trigger_trend_aligned = bool(structure.get("trigger_trend_aligned"))
        pattern_family = seed_state.pattern_family
        pattern_family_direction = seed_state.pattern_family_direction
        liquidity_sweep_buy = bool(structure.get("liquidity_sweep_buy"))
        liquidity_sweep_sell = bool(structure.get("liquidity_sweep_sell"))
        upside_exhaustion_score = float(structure.get("upside_exhaustion_score", 0.0) or 0.0)
        downside_exhaustion_score = float(structure.get("downside_exhaustion_score", 0.0) or 0.0)
        strong_generic_pattern_ready = seed_state.strong_generic_pattern_ready
        pattern_driven_direction = seed_state.pattern_driven_direction
        if structure_bias not in {"buy", "sell"} and not context_driven_direction and not pattern_driven_direction:
            return None
        inactivity_profile = inactivity_profile if isinstance(inactivity_profile, dict) else {}
        inactivity_relief_strength = _safe_float(inactivity_profile.get("relief_strength"), 0.0)
        inactivity_flat_book = bool(inactivity_profile.get("flat_book")) or bool(inactivity_profile.get("equity_relief"))
        inactivity_seed_relief = bool(inactivity_flat_book and inactivity_relief_strength > 0.0)

        directional_pullback = pullback_score if direction == "BUY" else -pullback_score
        directional_breakout = breakout_score if direction == "BUY" else -breakout_score
        context_confluence = _context_directional_confluence(context, direction)
        confluence_score = _safe_float(context_confluence.get("score"), 0.0)
        support_components = int(context_confluence.get("support_components", 0) or 0)
        conflict_components = int(context_confluence.get("conflict_components", 0) or 0)
        micro_context_support = _safe_float(context_confluence.get("micro_support"), 0.0)
        cross_context_support = _safe_float(context_confluence.get("cross_support"), 0.0)
        whale_context_support = _safe_float(context_confluence.get("whale_support"), 0.0)
        effective_alignment_score = alignment_score
        effective_setup_quality = setup_quality
        if context_driven_direction:
            effective_alignment_score = max(
                effective_alignment_score,
                max(0.0, abs(cross_context_support)),
                max(0.0, abs(micro_context_support) * 0.85),
            )
            effective_setup_quality = max(
                effective_setup_quality,
                0.20
                + max(0.0, abs(micro_context_support)) * 0.34
                + max(0.0, abs(cross_context_support)) * 0.18
                + max(0.0, abs(whale_context_support)) * 0.10,
            )
        if pattern_driven_direction:
            directional_impulse = max(0.0, abs(directional_breakout), abs(directional_pullback))
            effective_alignment_score = max(
                effective_alignment_score,
                0.22 + _clip(elite_pattern_rank) * 0.34 + directional_impulse * 0.20,
            )
            effective_setup_quality = max(
                effective_setup_quality,
                0.22 + _clip(elite_pattern_rank) * 0.28 + directional_impulse * 0.22,
            )
        effective_candle_quality_score = candle_quality_score
        effective_session_quality_score = session_quality_score
        if inactivity_seed_relief:
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.18 + effective_setup_quality * 0.34 + max(0.0, max(directional_breakout, directional_pullback)) * 0.16,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.20 + effective_alignment_score * 0.28 + target_efficiency_score * 0.20,
                )
        if context_driven_direction:
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.22
                    + effective_setup_quality * 0.22
                    + max(0.0, confluence_score) * 0.24
                    + max(0.0, abs(micro_context_support)) * 0.16,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.24 + effective_alignment_score * 0.16 + max(0.0, confluence_score) * 0.22,
                )
        pattern_family_ready = bool(
            pattern_family.startswith("trending_")
            and (
                entry_confirmation_ready
                or fast_entry_confirmation_ready
                or first_pullback_ready
                or breakout_retest_ready
                or elite_pattern_rank >= 0.28
                or strong_generic_pattern_ready
            )
        )
        if pattern_family_ready:
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.24 + effective_setup_quality * 0.20 + _clip(elite_pattern_rank) * 0.20,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.26 + effective_alignment_score * 0.18 + _clip(target_efficiency_score) * 0.18,
                )
        near_confirmation = _near_confirmation(entry_confirmation_count, entry_confirmation_bars_required)
        fast_confirmation_ready, fast_confirmation_count, fast_confirmation_required, _ = _effective_confirmation_gate(
            playbook="breakout_continuation",
            entry_confirmation_ready=entry_confirmation_ready,
            entry_confirmation_count=entry_confirmation_count,
            entry_confirmation_bars_required=entry_confirmation_bars_required,
            fast_entry_confirmation_ready=fast_entry_confirmation_ready,
            fast_entry_confirmation_count=fast_entry_confirmation_count,
            fast_entry_confirmation_bars_required=fast_entry_confirmation_bars_required,
        )
        near_fast_confirmation = _near_confirmation(fast_confirmation_count, fast_confirmation_required)
        context_has_true_depth = bool(context_confluence.get("depth_available"))
        context_has_synthetic_depth = bool(context_confluence.get("synthetic_depth"))
        context_depth_authority_tier = str(context_confluence.get("dom_authority_tier") or "").strip().lower()
        context_depth_stream_health = _safe_float(context_confluence.get("dom_stream_health_score"), 1.0)
        context_depth_stream_decay = _safe_float(context_confluence.get("dom_stream_trust_decay"), 0.0)
        context_depth_stream_degraded = bool(context_confluence.get("dom_stream_degraded"))
        context_fragmented_event_depth = bool(
            context_confluence.get("dom_ladder_ready")
            and (
                bool(context_confluence.get("dom_fragmented_market"))
                or context_depth_authority_tier in {"fragmented_event_ladder", "degraded_event_ladder"}
            )
        )
        context_has_event_backed_depth = bool(
            context_confluence.get("dom_ladder_ready")
            and not context_fragmented_event_depth
            and context_depth_stream_health >= 0.58
            and context_depth_stream_decay <= 0.35
            and not context_depth_stream_degraded
        )
        context_has_stream_snapshot_depth = bool(context_confluence.get("dom_stream_snapshot_ready"))
        context_depth_ready = bool(
            context_has_event_backed_depth
            or context_has_stream_snapshot_depth
            or context_has_true_depth
        )
        depth_pressure_profile = _depth_context_pressure_profile(category, context_confluence)
        directional_impulse = max(0.0, directional_breakout, directional_pullback)
        strict_family_directional_match = bool(
            (direction == "BUY" and pattern_family.startswith("trending_up_"))
            or (direction == "SELL" and pattern_family.startswith("trending_down_"))
        )
        live_flow_generic_override_source = ""
        if (
            pattern_family.endswith("generic")
            and not strict_family_directional_match
            and support_components >= 1
            and conflict_components == 0
            and directional_impulse >= 0.04
        ):
            if (
                context_has_event_backed_depth
                and confluence_score >= 0.18
                and micro_context_support >= 0.20
                and (
                    cross_context_support >= 0.10
                    or whale_context_support >= 0.16
                    or directional_impulse >= 0.08
                )
            ):
                live_flow_generic_override_source = "true_depth"
            elif (
                context_fragmented_event_depth
                and confluence_score >= 0.20
                and micro_context_support >= 0.22
                and (
                    cross_context_support >= 0.12
                    or whale_context_support >= 0.18
                    or directional_impulse >= 0.09
                )
            ):
                live_flow_generic_override_source = "fragmented_event_ladder"
            elif (
                context_confluence.get("dom_ladder_ready")
                and context_depth_authority_tier == "degraded_event_ladder"
                and confluence_score >= 0.22
                and micro_context_support >= 0.24
                and (
                    cross_context_support >= 0.14
                    or whale_context_support >= 0.20
                    or directional_impulse >= 0.10
                )
            ):
                live_flow_generic_override_source = "snapshot_depth"
            elif (
                context_has_true_depth
                and confluence_score >= (0.19 if context_has_stream_snapshot_depth else 0.20)
                and micro_context_support >= (0.21 if context_has_stream_snapshot_depth else 0.22)
                and (
                    cross_context_support >= 0.12
                    or whale_context_support >= 0.18
                    or directional_impulse >= (0.08 if context_has_stream_snapshot_depth else 0.09)
                )
            ):
                live_flow_generic_override_source = "snapshot_depth"
            elif (
                confluence_score >= 0.24
                and micro_context_support >= (0.22 if context_has_synthetic_depth else 0.24)
                and (
                    cross_context_support >= 0.16
                    or whale_context_support >= 0.22
                    or directional_impulse >= 0.10
                )
            ):
                live_flow_generic_override_source = "flow"
        live_flow_generic_override = bool(live_flow_generic_override_source)
        family_directional_match = bool(strict_family_directional_match or live_flow_generic_override)
        generic_alignment_floor = (
            max(0.52, float(plan.min_alignment_score) - 0.08)
            if live_flow_generic_override
            else max(0.56, float(plan.min_alignment_score) - 0.04)
        )
        generic_setup_floor = (
            max(0.50, float(plan.min_setup_quality) - 0.08)
            if live_flow_generic_override
            else max(0.52, float(plan.min_setup_quality) - 0.06)
        )
        generic_depth_override = live_flow_generic_override_source in {
            "true_depth",
            "fragmented_event_ladder",
            "snapshot_depth",
        }
        generic_target_floor = (
            0.30
            if live_flow_generic_override_source == "true_depth"
            else 0.305
            if live_flow_generic_override_source == "fragmented_event_ladder"
            else 0.31
            if live_flow_generic_override_source == "snapshot_depth"
            else 0.32
            if live_flow_generic_override
            else 0.40
        )
        generic_extension_ceiling = (
            1.55
            if live_flow_generic_override_source == "fragmented_event_ladder"
            else 1.60
            if live_flow_generic_override
            else 1.45
        )
        context_alignment_floor = (
            max(0.54, float(plan.min_alignment_score) - 0.08)
            if live_flow_generic_override
            else max(0.60, float(plan.min_alignment_score) - 0.04)
        )
        context_setup_floor = (
            max(0.52, float(plan.min_setup_quality) - 0.08)
            if live_flow_generic_override
            else max(0.56, float(plan.min_setup_quality) - 0.06)
        )
        if live_flow_generic_override:
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.20
                    + effective_setup_quality * 0.22
                    + max(0.0, confluence_score) * 0.20
                    + max(0.0, abs(micro_context_support)) * 0.18,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.22
                    + effective_alignment_score * 0.18
                    + max(0.0, confluence_score) * 0.18
                    + (
                        0.06
                        if context_has_event_backed_depth
                        else 0.04
                        if context_fragmented_event_depth
                        else 0.03
                        if context_has_true_depth
                        else 0.0
                    ),
                )
        structural_generic_rank_ready = bool(
            pattern_family.endswith("generic")
            and family_directional_match
            and elite_pattern_rank >= 0.18
            and alignment_score >= generic_alignment_floor
            and setup_quality >= generic_setup_floor
            and effective_candle_quality_score >= (0.30 if live_flow_generic_override else 0.34)
            and effective_session_quality_score >= (0.38 if live_flow_generic_override else 0.42)
            and extension_score <= min(1.55, generic_extension_ceiling)
            and target_efficiency_score >= max(0.34, generic_target_floor)
            and impulse_age_bars <= (6 if live_flow_generic_override else 5)
        )
        premium_generic_trend_ready = bool(
            pattern_family.endswith("generic")
            and family_directional_match
            and alignment_score >= 0.86
            and setup_quality >= 0.78
            and target_efficiency_score >= 0.55
            and extension_score <= 1.18
            and impulse_age_bars <= 5
            and (
                (direction == "BUY" and upside_exhaustion_score <= 0.50)
                or (direction == "SELL" and downside_exhaustion_score <= 0.50)
            )
        )
        potential_generic_trend_ready = bool(
            pattern_family.endswith("generic")
            and family_directional_match
            and alignment_score >= generic_alignment_floor
            and setup_quality >= generic_setup_floor
            and extension_score <= generic_extension_ceiling
            and target_efficiency_score >= generic_target_floor
            and impulse_age_bars <= (6 if live_flow_generic_override else 5)
            and (
                (
                    effective_candle_quality_score >= (0.30 if live_flow_generic_override else 0.34)
                    and effective_session_quality_score >= (0.38 if live_flow_generic_override else 0.42)
                )
                or premium_generic_trend_ready
            )
            and (
                max(directional_breakout, directional_pullback) >= 0.08
                or structural_generic_rank_ready
                or premium_generic_trend_ready
                or strong_generic_pattern_ready
            )
        )
        directional_liquidity_sweep_ready = bool(
            pattern_family.endswith("liquidity_sweep")
            and (
                (direction == "BUY" and liquidity_sweep_buy)
                or (direction == "SELL" and liquidity_sweep_sell)
                or family_directional_match
                or (directional_breakout >= 0.0 and alignment_score >= max(0.54, float(plan.min_alignment_score) - 0.06))
            )
        )
        relaxed_target_gate = bool(
            directional_liquidity_sweep_ready
            or (
                near_confirmation
                and (
                    pattern_family.endswith("liquidity_sweep")
                    or pattern_family.endswith("first_pullback")
                    or pattern_family.endswith("breakout_retest")
                )
            )
        )
        if directional_liquidity_sweep_ready:
            target_efficiency_floor = 0.0
            extension_ceiling = 1.12
            alignment_floor = max(0.54, float(plan.min_alignment_score) - 0.06)
            setup_floor = max(0.50, float(plan.min_setup_quality) - 0.08)
        elif potential_generic_trend_ready:
            target_efficiency_floor = generic_target_floor
            extension_ceiling = generic_extension_ceiling
            alignment_floor = generic_alignment_floor
            setup_floor = generic_setup_floor
        else:
            target_efficiency_floor = 0.20 if relaxed_target_gate else 0.32
            extension_ceiling = 1.10 if relaxed_target_gate else 1.05
            alignment_floor = max(0.52, float(plan.min_alignment_score) - 0.02)
            setup_floor = max(0.50, float(plan.min_setup_quality) - 0.02)
        if inactivity_seed_relief:
            target_efficiency_floor = max(0.0, target_efficiency_floor - (0.04 + inactivity_relief_strength * 0.08))
            extension_ceiling += 0.05 + inactivity_relief_strength * 0.10
            alignment_floor = max(0.50, alignment_floor - (0.02 + inactivity_relief_strength * 0.04))
            setup_floor = max(0.48, setup_floor - (0.02 + inactivity_relief_strength * 0.04))
        crypto_directional_relief_ready = bool(
            category == "crypto"
            and inactivity_seed_relief
            and pattern_family.endswith("generic")
            and family_directional_match
            and alignment_score >= 0.82
            and setup_quality >= 0.60
            and max(directional_breakout, directional_pullback) >= 0.06
            and impulse_age_bars <= 6
            and cluster_penalty <= 0.14
        )
        context_continuation_ready = bool(
            support_components >= 1
            and conflict_components == 0
            and confluence_score >= 0.18
            and alignment_score >= context_alignment_floor
            and setup_quality >= context_setup_floor
            and pattern_family.endswith("generic")
            and family_directional_match
            and impulse_age_bars <= 6
            and cluster_penalty <= 0.16
            and (
                max(directional_breakout, directional_pullback) >= 0.04
                or micro_context_support >= 0.22
                or cross_context_support >= 0.22
                or whale_context_support >= 0.28
            )
        )
        strong_context_continuation_ready = bool(
            context_continuation_ready
            and confluence_score >= 0.28
            and (
                support_components >= 2
                or micro_context_support >= 0.30
                or cross_context_support >= 0.30
            )
        )
        depth_context_pressure_ready = bool(
            bool(depth_pressure_profile.get("ready"))
            and pattern_family.endswith("generic")
            and family_directional_match
            and structure_bias in {"buy", "sell"}
            and support_components >= 1
            and conflict_components == 0
            and context_depth_ready
            and confluence_score >= _safe_float(depth_pressure_profile.get("confluence_floor"), 0.16)
            and max(
                abs(micro_context_support),
                abs(cross_context_support),
                abs(whale_context_support),
            )
            >= _safe_float(depth_pressure_profile.get("flow_floor"), 0.28)
            and alignment_score >= _safe_float(depth_pressure_profile.get("alignment_floor"), 0.42)
            and setup_quality >= _safe_float(depth_pressure_profile.get("setup_floor"), 0.50)
            and target_efficiency_score >= _safe_float(depth_pressure_profile.get("target_floor"), 0.50)
            and extension_score <= _safe_float(depth_pressure_profile.get("extension_ceiling"), 1.05)
            and impulse_age_bars <= 6
            and cluster_penalty <= 0.18
        )
        depth_breakout_pattern_driven_ready = bool(
            pattern_driven_direction
            and pattern_family_direction == direction
        )
        directional_pattern_family_ready = bool(
            pattern_family_ready
            and pattern_family_direction == direction
        )
        depth_breakout_direction_ready = bool(
            structure_bias in {"buy", "sell"}
            or depth_breakout_pattern_driven_ready
            or directional_pattern_family_ready
            or (
                strict_family_directional_match
                and elite_pattern_rank >= 0.18
            )
        )
        depth_breakout_flow_floor = _safe_float(
            depth_pressure_profile.get("breakout_ignition_flow_floor"),
            0.22,
        )
        depth_breakout_flow_support = max(
            abs(micro_context_support),
            abs(cross_context_support),
            abs(whale_context_support),
        )
        depth_breakout_ignition_ready = bool(
            bool(depth_pressure_profile.get("ready"))
            and depth_breakout_direction_ready
            and (
                family_directional_match
                or depth_breakout_pattern_driven_ready
                or directional_pattern_family_ready
            )
            and support_components >= 1
            and conflict_components == 0
            and context_depth_ready
            and confluence_score >= _safe_float(depth_pressure_profile.get("breakout_ignition_confluence_floor"), 0.12)
            and depth_breakout_flow_support >= depth_breakout_flow_floor
            and effective_alignment_score >= _safe_float(depth_pressure_profile.get("alignment_floor"), 0.42)
            and effective_setup_quality >= _safe_float(depth_pressure_profile.get("setup_floor"), 0.50)
            and target_efficiency_score >= _safe_float(depth_pressure_profile.get("breakout_ignition_target_floor"), 0.08)
            and extension_score <= _safe_float(depth_pressure_profile.get("breakout_ignition_extension_ceiling"), 1.38)
            and impulse_age_bars <= int(depth_pressure_profile.get("breakout_ignition_impulse_age_limit", 7) or 7)
            and cluster_penalty <= 0.20
            and (
                max(directional_breakout, directional_pullback) >= 0.06
                or micro_context_support >= depth_breakout_flow_floor
                or (
                    depth_breakout_pattern_driven_ready
                    and elite_pattern_rank >= 0.28
                    and depth_breakout_flow_support >= depth_breakout_flow_floor
                )
            )
        )
        depth_momentum_continuation_ready = bool(
            bool(depth_pressure_profile.get("ready"))
            and depth_breakout_direction_ready
            and (
                family_directional_match
                or depth_breakout_pattern_driven_ready
                or directional_pattern_family_ready
            )
            and support_components >= 1
            and conflict_components == 0
            and context_depth_ready
            and confluence_score >= 0.30
            and depth_breakout_flow_support >= max(0.34, depth_breakout_flow_floor)
            and effective_alignment_score >= 0.58
            and effective_setup_quality >= 0.62
            and target_efficiency_score >= 0.50
            and extension_score <= 1.35
            and cluster_penalty <= 0.18
            and trigger_trend_aligned
            and self._trend_sign(str(structure.get("trend_5m", "") or "")) == _playbook_direction_sign(direction)
        )
        if crypto_directional_relief_ready:
            target_efficiency_floor = min(
                target_efficiency_floor,
                0.0 if alignment_score >= 0.92 else 0.16,
            )
            extension_ceiling = max(
                extension_ceiling,
                2.10 if target_efficiency_score >= 0.82 else 1.75,
            )
            alignment_floor = min(alignment_floor, 0.54)
            setup_floor = min(setup_floor, 0.50)
        if context_continuation_ready:
            target_efficiency_floor = min(
                target_efficiency_floor,
                0.08 if strong_context_continuation_ready else 0.12,
            )
            extension_ceiling = max(
                extension_ceiling,
                2.20 if strong_context_continuation_ready else 1.85,
            )
            alignment_floor = min(alignment_floor, 0.56)
            setup_floor = min(setup_floor, 0.52)
        if context_driven_direction:
            target_efficiency_floor = min(target_efficiency_floor, 0.0)
            extension_ceiling = max(extension_ceiling, 1.75 if confluence_score >= 0.30 else 1.50)
            alignment_floor = min(alignment_floor, 0.36)
            setup_floor = min(setup_floor, 0.30)
        if pattern_family_ready:
            target_efficiency_floor = min(target_efficiency_floor, 0.0)
            extension_ceiling = max(extension_ceiling, 1.70)
            alignment_floor = min(alignment_floor, 0.44)
            setup_floor = min(setup_floor, 0.36)
        if pattern_driven_direction:
            alignment_floor = min(alignment_floor, 0.30)
            setup_floor = min(setup_floor, 0.24)
        if depth_context_pressure_ready:
            target_efficiency_floor = min(target_efficiency_floor, _safe_float(depth_pressure_profile.get("target_floor"), 0.50))
            extension_ceiling = max(extension_ceiling, max(1.35, _safe_float(depth_pressure_profile.get("extension_ceiling"), 1.05)))
            alignment_floor = min(alignment_floor, _safe_float(depth_pressure_profile.get("alignment_floor"), 0.42))
            setup_floor = min(setup_floor, _safe_float(depth_pressure_profile.get("setup_floor"), 0.50))
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.20
                    + effective_setup_quality * 0.20
                    + max(0.0, confluence_score) * 0.16
                    + max(0.0, abs(micro_context_support)) * 0.18,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.22
                    + effective_alignment_score * 0.12
                    + max(0.0, confluence_score) * 0.16
                    + max(0.0, target_efficiency_score) * 0.12,
                )
        if depth_breakout_ignition_ready:
            target_efficiency_floor = min(
                target_efficiency_floor,
                _safe_float(depth_pressure_profile.get("breakout_ignition_target_floor"), 0.08),
            )
            extension_ceiling = max(
                extension_ceiling,
                _safe_float(depth_pressure_profile.get("breakout_ignition_extension_ceiling"), 1.38),
            )
            alignment_floor = min(alignment_floor, _safe_float(depth_pressure_profile.get("alignment_floor"), 0.42))
            setup_floor = min(setup_floor, _safe_float(depth_pressure_profile.get("setup_floor"), 0.50))
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.20
                    + effective_setup_quality * 0.18
                    + max(0.0, confluence_score) * 0.18
                    + max(0.0, abs(micro_context_support)) * 0.20,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.22
                    + effective_alignment_score * 0.12
                    + max(0.0, confluence_score) * 0.18
                    + max(0.0, target_efficiency_score) * 0.10,
                )
        if depth_momentum_continuation_ready:
            target_efficiency_floor = min(target_efficiency_floor, 0.50)
            extension_ceiling = max(extension_ceiling, 1.35)
            alignment_floor = min(alignment_floor, 0.58)
            setup_floor = min(setup_floor, 0.62)
            if effective_candle_quality_score <= 0.0:
                effective_candle_quality_score = min(
                    1.0,
                    0.24
                    + effective_setup_quality * 0.18
                    + max(0.0, confluence_score) * 0.18
                    + max(0.0, abs(micro_context_support)) * 0.18,
                )
            if effective_session_quality_score <= 0.0 and str(session or "").lower() != "off":
                effective_session_quality_score = min(
                    1.0,
                    0.24
                    + effective_alignment_score * 0.14
                    + max(0.0, confluence_score) * 0.18
                    + max(0.0, target_efficiency_score) * 0.10,
                )

        if effective_alignment_score < alignment_floor:
            return None
        if effective_setup_quality < setup_floor:
            return None
        candle_floor = 0.30
        session_floor = 0.34
        if inactivity_seed_relief:
            candle_floor = max(0.20, candle_floor - (0.04 + inactivity_relief_strength * 0.04))
            session_floor = max(0.24, session_floor - (0.04 + inactivity_relief_strength * 0.05))
        if crypto_directional_relief_ready:
            candle_floor = max(0.18, candle_floor - 0.06)
            session_floor = max(0.20, session_floor - 0.08)
        if depth_context_pressure_ready:
            candle_floor = max(0.18, candle_floor - 0.08)
            session_floor = max(0.20, session_floor - 0.08)
        if depth_breakout_ignition_ready:
            candle_floor = max(0.18, candle_floor - 0.08)
            session_floor = max(0.20, session_floor - 0.08)
        if context_continuation_ready:
            candle_floor = max(0.18, candle_floor - (0.04 if strong_context_continuation_ready else 0.02))
            session_floor = max(0.20, session_floor - (0.06 if strong_context_continuation_ready else 0.04))
        if context_driven_direction:
            candle_floor = max(0.18, candle_floor - 0.08)
            session_floor = max(0.20, session_floor - 0.08)
        if live_flow_generic_override:
            candle_floor = max(
                0.18,
                candle_floor - (0.08 if context_has_event_backed_depth else 0.07 if generic_depth_override else 0.06),
            )
            session_floor = max(
                0.20,
                session_floor - (0.10 if context_has_event_backed_depth else 0.09 if generic_depth_override else 0.08),
            )
        if pattern_family_ready:
            candle_floor = max(0.18, candle_floor - 0.08)
            session_floor = max(0.20, session_floor - 0.08)
        if not premium_generic_trend_ready and (
            effective_candle_quality_score < candle_floor or effective_session_quality_score < session_floor
        ):
            return None
        if extension_score > extension_ceiling or target_efficiency_score < target_efficiency_floor:
            return None
        impulse_age_limit = 6 + (1 if inactivity_seed_relief and inactivity_relief_strength >= 0.35 else 0) + (
            1 if inactivity_seed_relief and inactivity_relief_strength >= 0.75 else 0
        )
        cluster_penalty_limit = 0.26 + (0.03 + inactivity_relief_strength * 0.04 if inactivity_seed_relief else 0.0)
        if impulse_age_bars >= impulse_age_limit or cluster_penalty >= cluster_penalty_limit:
            if not (
                (
                    depth_breakout_ignition_ready
                    and impulse_age_bars <= int(depth_pressure_profile.get("breakout_ignition_impulse_age_limit", 7) or 7)
                    and cluster_penalty < 0.20
                )
                or depth_momentum_continuation_ready
            ):
                return None
        context_fast_track_ready = bool(
            context_continuation_ready
            and (
                strong_context_continuation_ready
                or max(
                    abs(micro_context_support),
                    abs(cross_context_support),
                    abs(whale_context_support),
                )
                >= 0.30
            )
            and target_efficiency_score >= max(0.08, target_efficiency_floor - 0.08)
            and extension_score <= max(
                extension_ceiling,
                2.20 if strong_context_continuation_ready else 1.85,
            )
        )
        context_pressure_confirmation_ready = bool(
            entry_confirmation_ready
            or fast_entry_confirmation_ready
            or near_confirmation
            or near_fast_confirmation
            or (
                entry_confirmation_bars_required <= 1
                and fast_confirmation_required <= 1
                and target_efficiency_score >= 0.18
                and max(
                    abs(micro_context_support),
                    abs(cross_context_support),
                    abs(whale_context_support),
                )
                >= 0.36
            )
        )
        context_pressure_execution_ready = bool(
            context_driven_direction
            and alignment_score >= max(0.60, float(plan.min_alignment_score))
            and setup_quality >= max(0.56, float(plan.min_setup_quality))
            and candle_quality_score >= 0.30
            and session_quality_score >= 0.36
            and target_efficiency_score >= 0.10
            and extension_score <= 1.24
            and impulse_age_bars <= 5
            and cluster_penalty <= 0.20
            and context_pressure_confirmation_ready
        )
        if context_driven_direction and not context_pressure_execution_ready and not depth_breakout_ignition_ready:
            return None
        if (
            fast_confirmation_required > 1
            and not fast_confirmation_ready
            and not near_fast_confirmation
            and not directional_liquidity_sweep_ready
            and not context_fast_track_ready
            and not depth_context_pressure_ready
            and not depth_breakout_ignition_ready
            and not depth_momentum_continuation_ready
        ):
            return None
        if direction == "BUY" and upside_exhaustion_score >= 0.58:
            return None
        if direction == "SELL" and downside_exhaustion_score >= 0.58:
            return None

        playbook = ""
        entry_style = ""
        readiness_note = ""
        liquidity_sweep_directional = directional_liquidity_sweep_ready
        emerging_sweep_ready = bool(
            directional_liquidity_sweep_ready
            and alignment_score >= max(0.54, float(plan.min_alignment_score) - 0.06)
            and setup_quality >= max(0.50, float(plan.min_setup_quality) - 0.06)
            and effective_candle_quality_score >= 0.28
            and effective_session_quality_score >= 0.42
            and extension_score <= 1.02
            and target_efficiency_score >= 0.0
            and impulse_age_bars <= 5
        )
        emerging_continuation_ready = bool(
            alignment_score >= max(0.54, float(plan.min_alignment_score) - 0.06)
            and setup_quality >= max(0.50, float(plan.min_setup_quality) - 0.06)
            and effective_candle_quality_score >= 0.30
            and effective_session_quality_score >= 0.46
            and extension_score <= 1.00
            and target_efficiency_score >= 0.20
            and impulse_age_bars <= 5
        )
        emerging_generic_trend_ready = bool(potential_generic_trend_ready)
        if (
            inactivity_seed_relief
            and pattern_family.endswith("generic")
            and family_directional_match
            and alignment_score >= max(0.58, float(plan.min_alignment_score) - 0.06)
            and setup_quality >= max(0.56, float(plan.min_setup_quality) - 0.06)
            and effective_candle_quality_score >= max(0.18, candle_floor - 0.02)
            and effective_session_quality_score >= max(0.24, session_floor - 0.02)
            and extension_score <= extension_ceiling
            and target_efficiency_score >= max(0.0, target_efficiency_floor)
            and impulse_age_bars <= impulse_age_limit
        ):
            emerging_generic_trend_ready = True
        if crypto_directional_relief_ready and (
            target_efficiency_score >= max(0.0, target_efficiency_floor)
            or alignment_score >= 0.92
        ):
            emerging_generic_trend_ready = True
        if failed_opposite_move_confirmed and "failed_break_reclaim" in plan.allowed_playbooks:
            playbook = "failed_break_reclaim"
            entry_style = "failed_move_reclaim"
            readiness_note = "failed_opposite_move_confirmed"
        elif first_pullback_ready and pattern_driven_direction and "trend_pullback" in plan.allowed_playbooks:
            playbook = "trend_pullback"
            entry_style = "elite_pattern_pullback"
            readiness_note = "pattern_pullback_ready"
        elif breakout_retest_ready and pattern_driven_direction and "breakout_retest" in plan.allowed_playbooks:
            playbook = "breakout_retest"
            entry_style = "elite_pattern_retest"
            readiness_note = "pattern_retest_ready"
        elif breakout_retest_ready and directional_breakout >= 0.18 and "breakout_retest" in plan.allowed_playbooks:
            playbook = "breakout_retest"
            entry_style = "elite_retest_ready"
            readiness_note = "breakout_retest_ready"
        elif first_pullback_ready and directional_pullback >= 0.18 and "trend_pullback" in plan.allowed_playbooks:
            playbook = "trend_pullback"
            entry_style = "elite_pullback_ready"
            readiness_note = "first_pullback_ready"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and pattern_family.endswith("liquidity_sweep")
            and liquidity_sweep_directional
            and max(directional_breakout, directional_pullback) >= 0.08
            and emerging_sweep_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_sweep_continuation"
            readiness_note = "liquidity_sweep_continuation"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and near_fast_confirmation
            and directional_breakout >= 0.16
            and emerging_continuation_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_early_continuation"
            readiness_note = "early_continuation_ready"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and context_continuation_ready
            and not live_flow_generic_override
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_context_continuation"
            readiness_note = "context_flow_continuation"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and live_flow_generic_override
            and pattern_family.endswith("generic")
            and support_components >= 1
            and conflict_components == 0
            and confluence_score >= 0.24
            and max(directional_breakout, directional_pullback) >= 0.08
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_flow_continuation"
            readiness_note = f"generic_flow_{live_flow_generic_override_source}"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and depth_context_pressure_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_context_pressure"
            readiness_note = f"depth_context_pressure_{depth_pressure_profile.get('kind') or 'depth'}"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and depth_breakout_ignition_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "breakout_ignition"
            readiness_note = f"breakout_ignition_{depth_pressure_profile.get('kind') or 'depth'}"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and depth_momentum_continuation_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "breakout_ignition"
            readiness_note = f"breakout_ignition_momentum_{depth_pressure_profile.get('kind') or 'depth'}"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and context_driven_direction
            and context_pressure_execution_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_context_pressure"
            readiness_note = "dominant_context_pressure"
        elif (
            inactivity_seed_relief
            and "breakout_continuation" in plan.allowed_playbooks
            and pattern_family.endswith("generic")
            and family_directional_match
            and alignment_score >= max(0.58, float(plan.min_alignment_score) - 0.06)
            and setup_quality >= max(0.56, float(plan.min_setup_quality) - 0.06)
            and effective_candle_quality_score >= max(0.18, candle_floor - 0.02)
            and effective_session_quality_score >= max(0.24, session_floor - 0.02)
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_trend_continuation"
            readiness_note = "inactivity_generic_continuation"
        elif (
            "breakout_continuation" in plan.allowed_playbooks
            and emerging_generic_trend_ready
        ):
            playbook = "breakout_continuation"
            entry_style = "elite_trend_continuation"
            readiness_note = "generic_trend_continuation"
        elif entry_confirmation_ready and directional_breakout >= 0.48 and "breakout_continuation" in plan.allowed_playbooks:
            playbook = "breakout_continuation"
            entry_style = "elite_breakout_ready"
            readiness_note = "entry_confirmation_ready"

        if not playbook:
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        structural_ready_bonus = 0.0
        if pattern_family.endswith("liquidity_sweep") and liquidity_sweep_directional:
            structural_ready_bonus += 0.08
        elif pattern_family.endswith("first_pullback"):
            structural_ready_bonus += 0.06
        elif pattern_family.endswith("breakout_retest"):
            structural_ready_bonus += 0.06
        elif entry_style == "elite_trend_continuation":
            structural_ready_bonus += 0.05
            if structural_generic_rank_ready:
                structural_ready_bonus += 0.03
            if premium_generic_trend_ready:
                structural_ready_bonus += 0.02
            if live_flow_generic_override:
                structural_ready_bonus += 0.04 + (0.02 if context_has_event_backed_depth else 0.01 if generic_depth_override else 0.0)
            if inactivity_seed_relief:
                structural_ready_bonus += 0.03 + inactivity_relief_strength * 0.03
        elif entry_style == "elite_context_continuation":
            structural_ready_bonus += 0.06 if strong_context_continuation_ready else 0.04
        elif entry_style == "elite_flow_continuation":
            structural_ready_bonus += 0.06 + (0.02 if context_has_event_backed_depth else 0.01 if generic_depth_override else 0.0)
        elif entry_style == "elite_context_pressure":
            structural_ready_bonus += 0.08 + (0.02 if depth_context_pressure_ready else 0.0)
        elif readiness_note.startswith("breakout_ignition_momentum_"):
            structural_ready_bonus += 0.16
        elif readiness_note.startswith("breakout_ignition_"):
            structural_ready_bonus += 0.10
        elif entry_style in {"elite_pattern_pullback", "elite_pattern_retest"}:
            structural_ready_bonus += 0.10
        if near_confirmation:
            structural_ready_bonus += 0.05
        context_continuation_bonus = entry_style == "elite_context_continuation"
        context_pressure_bonus = entry_style == "elite_context_pressure"
        live_flow_continuation_bonus = entry_style in {"elite_trend_continuation", "elite_flow_continuation"} and live_flow_generic_override
        score = _clip(
            abs(directional_breakout) * 0.22
            + abs(directional_pullback) * 0.18
            + _clip(effective_setup_quality) * 0.18
            + _clip(effective_alignment_score) * 0.16
            + _clip(effective_candle_quality_score) * 0.10
            + _clip(effective_session_quality_score) * 0.08
            + _clip(target_efficiency_score) * 0.08
            + _clip(elite_pattern_rank) * 0.10
            + (
                max(0.0, confluence_score)
                * (
                    0.18
                    if context_driven_direction
                    else 0.14
                    if context_pressure_bonus
                    else 0.14
                    if context_continuation_bonus
                    else 0.10
                    if live_flow_continuation_bonus
                    else 0.0
                )
            )
            + (
                max(0.0, abs(micro_context_support))
                * (
                    0.10
                    if context_driven_direction
                    else 0.10
                    if context_pressure_bonus
                    else 0.08
                    if context_continuation_bonus
                    else 0.08
                    if live_flow_continuation_bonus
                    else 0.0
                )
            )
            + (
                max(0.0, abs(cross_context_support))
                * (
                    0.06
                    if context_driven_direction
                    else 0.05
                    if context_pressure_bonus
                    else 0.05
                    if context_continuation_bonus
                    else 0.04
                    if live_flow_continuation_bonus
                    else 0.0
                )
            )
            + (0.06 if failed_opposite_move_confirmed else 0.0)
            + (0.05 if breakout_retest_ready else 0.0)
            + (0.04 if first_pullback_ready else 0.0)
            + structural_ready_bonus
            - min(0.12, extension_score * 0.06)
            - min(0.08, cluster_penalty * 0.30),
            0.0,
            1.0,
        )
        score_floor = 0.56
        if entry_style == "elite_sweep_continuation":
            score_floor = 0.30
        elif entry_style == "elite_early_continuation":
            score_floor = 0.52
        elif entry_style == "elite_trend_continuation":
            score_floor = 0.40
            if live_flow_generic_override:
                score_floor = 0.34 if context_has_event_backed_depth else 0.35 if generic_depth_override else 0.36
            if inactivity_seed_relief:
                score_floor = max(0.28, score_floor - (0.04 + inactivity_relief_strength * 0.08))
        elif entry_style == "elite_context_continuation":
            score_floor = 0.42 if strong_context_continuation_ready else 0.46
        elif entry_style == "elite_flow_continuation":
            score_floor = 0.34 if context_has_event_backed_depth else 0.35 if generic_depth_override else 0.36
        elif entry_style == "elite_context_pressure":
            score_floor = 0.40 if depth_context_pressure_ready else 0.40 if confluence_score >= 0.32 else 0.44
        elif readiness_note.startswith("breakout_ignition_momentum_"):
            score_floor = 0.34
        elif readiness_note.startswith("breakout_ignition_"):
            score_floor = 0.38
        elif entry_style in {"elite_pattern_pullback", "elite_pattern_retest"}:
            score_floor = 0.40 if elite_pattern_rank >= 0.45 else 0.44
        if score < score_floor:
            return None

        confidence = _clip(
            0.42
            + score * 0.40
            + max(0.0, confluence_score) * 0.08
            + (0.05 if entry_confirmation_ready else 0.0)
            + (0.03 if readiness_note.startswith("breakout_ignition_momentum_") else 0.0)
            + (0.03 if playbook in {"breakout_retest", "trend_pullback", "failed_break_reclaim"} else 0.0),
            0.0,
            0.93,
        )
        return {
            "playbook": playbook,
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "context_confluence": round(confluence_score, 4),
            "cross_alignment": round(cross_context_support, 4),
            "cross_confidence": round(_safe_float(context_confluence.get("cross_confidence"), 0.0), 4),
            "micro_score": round(micro_context_support, 4),
            "whale_context_support": round(whale_context_support, 4),
            "support_components": support_components,
            "conflict_components": conflict_components,
            "entry_style": entry_style,
            "generic_flow_override": bool(live_flow_generic_override),
            "generic_flow_override_source": (
                str(live_flow_generic_override_source or "")
                if live_flow_generic_override
                else ""
            ),
            "dom_stream_snapshot_ready": bool(context_confluence.get("dom_stream_snapshot_ready")),
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, playbook, asset=asset, category=category),
            "notes": [
                "elite_ready_fallback",
                readiness_note,
                f"session={session}",
                f"align={effective_alignment_score:.2f}",
                f"setup={effective_setup_quality:.2f}",
                f"ctx={confluence_score:+.2f}/micro={micro_context_support:+.2f}/cross={cross_context_support:+.2f}",
                (
                    f"generic_flow_override={live_flow_generic_override_source}"
                    if live_flow_generic_override
                    else "generic_flow_override=off"
                ),
                "depth_context_pressure=1" if depth_context_pressure_ready else "depth_context_pressure=0",
                "breakout_ignition=1" if depth_breakout_ignition_ready else "breakout_ignition=0",
                "depth_momentum_continuation=1" if depth_momentum_continuation_ready else "depth_momentum_continuation=0",
                f"depth_profile={depth_pressure_profile.get('kind') or 'none'}",
                f"inactivity_relief={inactivity_relief_strength:.2f}" if inactivity_seed_relief else "inactivity_relief=0.00",
            ],
        }

    def _breakout_continuation(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        preferred_interval = self.preferred_interval(category, asset)
        seed_state = _build_seed_state(structure=structure, plan=plan, context=context)
        lookback = min(profile.breakout_lookback, max(8, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_close = float(latest["close"])
        latest_open = float(latest["open"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        current_body = abs(latest_close - latest_open)
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        atr = self._atr(frame.tail(max(20, lookback + 2)))
        range_span = max(range_high - range_low, atr, 1e-9)

        breakout_up = max(0.0, latest_close - range_high)
        breakout_down = max(0.0, range_low - latest_close)
        wick_up = max(0.0, latest_high - range_high)
        wick_down = max(0.0, range_low - latest_low)

        if breakout_up <= 0.0 and breakout_down <= 0.0:
            return None

        direction = "BUY" if breakout_up >= breakout_down else "SELL"
        structure_bias = seed_state.structure_bias
        pattern_family = seed_state.pattern_family
        if seed_state.direction == "BUY" and breakout_up > 0.0:
            if breakout_down <= breakout_up * 1.45:
                direction = "BUY"
        elif seed_state.direction == "SELL" and breakout_down > 0.0:
            if breakout_up <= breakout_down * 1.45:
                direction = "SELL"
        elif structure_bias == "buy" and breakout_up > 0.0 and pattern_family.startswith("trending_up_"):
            if breakout_down <= breakout_up * 1.45:
                direction = "BUY"
        elif structure_bias == "sell" and breakout_down > 0.0 and pattern_family.startswith("trending_down_"):
            if breakout_up <= breakout_down * 1.45:
                direction = "SELL"
        breakout_dist = breakout_up if direction == "BUY" else breakout_down
        breakout_wick = wick_up if direction == "BUY" else wick_down
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        volatility_state = str(structure.get("volatility_state", "unknown") or "unknown").lower()
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        effective_alignment_score = max(
            alignment_score,
            0.22 + _clip(seed_state.elite_pattern_rank) * 0.24 if seed_state.pattern_driven_direction and seed_state.direction == direction else alignment_score,
        )
        effective_setup_quality = max(
            setup_quality,
            0.22
            + _clip(seed_state.elite_pattern_rank) * 0.20
            + max(0.0, seed_state.context_confluence) * 0.10
            if (seed_state.pattern_driven_direction or seed_state.context_driven_direction) and seed_state.direction == direction
            else setup_quality,
        )
        entry_style = "breakout_close"
        structural_bonus = 0.0
        if seed_state.pattern_driven_direction and seed_state.direction == direction:
            entry_style = "pattern_breakout_followthrough"
            structural_bonus += 0.08
        elif seed_state.context_driven_direction and seed_state.direction == direction:
            entry_style = "context_breakout_followthrough"
            structural_bonus += 0.06
        elif seed_state.strong_generic_pattern_ready and seed_state.direction == direction:
            entry_style = "generic_breakout_followthrough"
            structural_bonus += 0.05

        direction_breakout = breakout_score if direction == "BUY" else -breakout_score
        breakout_norm = _clip(breakout_dist / max(atr * 0.75, range_span * 0.18, 1e-9))
        body_norm = _clip(current_body / max(avg_body * 2.0, 1e-9))
        wick_confirm = _clip((breakout_dist + breakout_wick) / max(atr, 1e-9))
        structure_component = _clip(direction_breakout, 0.0, 1.0)
        regime_component = 0.72 if (
            (direction == "BUY" and regime == "trending_up")
            or (direction == "SELL" and regime == "trending_down")
        ) else 0.55 if volatility_state in {"expansion", "normal"} else 0.40

        score = (
            breakout_norm * 0.34
            + body_norm * 0.20
            + wick_confirm * 0.10
            + _clip(effective_setup_quality) * 0.16
            + _clip(effective_alignment_score) * 0.10
            + structure_component * 0.10
            + structural_bonus
        )
        confidence = _clip(0.42 + score * 0.40 + regime_component * 0.18, 0.0, 0.95)

        score_floor = profile.breakout_min_score
        if entry_style == "pattern_breakout_followthrough":
            score_floor = max(0.48, score_floor - 0.08)
        elif entry_style in {"context_breakout_followthrough", "generic_breakout_followthrough"}:
            score_floor = max(0.50, score_floor - 0.06)
        if score < score_floor:
            return None

        notes = [
            "range_break",
            f"session={session}",
            f"body_x={current_body / max(avg_body, 1e-9):.2f}",
            f"breakout_atr={breakout_dist / max(atr, 1e-9):.2f}",
        ]
        return {
            "playbook": "breakout_continuation",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": entry_style,
            "session": session,
            "preferred_interval": preferred_interval,
            "breakout_confirmed": True,
            "management": self._management_template(profile, "breakout_continuation", asset=asset, category=category),
            "notes": notes,
        }

    def _breakout_retest(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        preferred_interval = self.preferred_interval(category, asset)
        seed_state = _build_seed_state(structure=structure, plan=plan, context=context)
        if len(frame) < profile.breakout_lookback + profile.retest_window + 2:
            return None

        recent = frame.tail(profile.breakout_lookback + profile.retest_window + 2)
        base = recent.iloc[: -(profile.retest_window + 1)]
        if base.empty:
            return None
        prior_recent = recent.iloc[-(profile.retest_window + 1) : -1]
        latest = recent.iloc[-1]

        range_high = float(base["high"].max())
        range_low = float(base["low"].min())
        atr = self._atr(recent.tail(24))
        tolerance = max(atr * profile.retest_tolerance_atr, abs(range_high - range_low) * 0.08, 1e-9)

        buy_break_seen = any(float(value) > range_high for value in prior_recent["close"])
        sell_break_seen = any(float(value) < range_low for value in prior_recent["close"])

        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_low = float(latest["low"])
        latest_high = float(latest["high"])

        candidates: List[Dict[str, Any]] = []
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        preferred_direction = seed_state.direction
        allow_buy_candidate = preferred_direction in {"", "BUY"}
        allow_sell_candidate = preferred_direction in {"", "SELL"}

        if allow_buy_candidate and buy_break_seen and latest_low <= range_high + tolerance and latest_close >= range_high:
            hold_strength = _clip((latest_close - range_high + tolerance) / max(tolerance * 2.0, 1e-9))
            body_bias = _clip((latest_close - latest_open + tolerance) / max(tolerance * 2.5, 1e-9))
            structural_bonus = 0.0
            entry_style = "retest_hold"
            if preferred_direction == "BUY" and seed_state.pattern_driven_direction:
                entry_style = "pattern_retest_hold"
                structural_bonus += 0.08
            elif preferred_direction == "BUY" and seed_state.context_driven_direction:
                entry_style = "context_retest_hold"
                structural_bonus += 0.06
            score = (
                hold_strength * 0.34
                + body_bias * 0.16
                + _clip(alignment_score) * 0.15
                + _clip(setup_quality) * 0.15
                + _clip(breakout_score, 0.0, 1.0) * 0.10
                + (0.10 if regime == "trending_up" else 0.04)
                + structural_bonus
            )
            confidence = _clip(0.43 + score * 0.42, 0.0, 0.94)
            score_floor = profile.retest_min_score
            if entry_style == "pattern_retest_hold":
                score_floor = max(0.50, score_floor - 0.08)
            elif entry_style == "context_retest_hold":
                score_floor = max(0.52, score_floor - 0.06)
            if score >= score_floor:
                candidates.append(
                    {
                        "playbook": "breakout_retest",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": entry_style,
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "retest_confirmed": True,
                        "management": self._management_template(profile, "breakout_retest", asset=asset, category=category),
                        "notes": [
                            "retest_hold",
                            f"session={session}",
                            f"level={range_high:.6f}",
                            f"atr_tol={tolerance / max(atr, 1e-9):.2f}",
                        ],
                    }
                )

        if allow_sell_candidate and sell_break_seen and latest_high >= range_low - tolerance and latest_close <= range_low:
            hold_strength = _clip((range_low - latest_close + tolerance) / max(tolerance * 2.0, 1e-9))
            body_bias = _clip((latest_open - latest_close + tolerance) / max(tolerance * 2.5, 1e-9))
            structural_bonus = 0.0
            entry_style = "retest_hold"
            if preferred_direction == "SELL" and seed_state.pattern_driven_direction:
                entry_style = "pattern_retest_hold"
                structural_bonus += 0.08
            elif preferred_direction == "SELL" and seed_state.context_driven_direction:
                entry_style = "context_retest_hold"
                structural_bonus += 0.06
            score = (
                hold_strength * 0.34
                + body_bias * 0.16
                + _clip(alignment_score) * 0.15
                + _clip(setup_quality) * 0.15
                + _clip(-breakout_score, 0.0, 1.0) * 0.10
                + (0.10 if regime == "trending_down" else 0.04)
                + structural_bonus
            )
            confidence = _clip(0.43 + score * 0.42, 0.0, 0.94)
            score_floor = profile.retest_min_score
            if entry_style == "pattern_retest_hold":
                score_floor = max(0.50, score_floor - 0.08)
            elif entry_style == "context_retest_hold":
                score_floor = max(0.52, score_floor - 0.06)
            if score >= score_floor:
                candidates.append(
                    {
                        "playbook": "breakout_retest",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": entry_style,
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "retest_confirmed": True,
                        "management": self._management_template(profile, "breakout_retest", asset=asset, category=category),
                        "notes": [
                            "retest_hold",
                            f"session={session}",
                            f"level={range_low:.6f}",
                            f"atr_tol={tolerance / max(atr, 1e-9):.2f}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _trend_pullback(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        preferred_interval = self.preferred_interval(category, asset)
        seed_state = _build_seed_state(
            structure=structure,
            plan=plan,
            context=context,
            allow_breakout_direction=True,
        )
        structure_bias = seed_state.structure_bias
        pattern_family = seed_state.pattern_family
        direction = seed_state.direction
        if not direction:
            return None

        pullback_score = seed_state.pullback_score
        breakout_score = seed_state.breakout_score
        alignment_score = seed_state.alignment_score
        setup_quality = seed_state.setup_quality
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        trend_15m = str(structure.get("trend_15m", "unknown") or "unknown").lower()
        trend_1h = str(structure.get("trend_1h", "unknown") or "unknown").lower()
        upside_exhaustion_score = float(structure.get("upside_exhaustion_score", 0.0) or 0.0)
        downside_exhaustion_score = float(structure.get("downside_exhaustion_score", 0.0) or 0.0)
        first_pullback_ready = seed_state.first_pullback_ready
        entry_confirmation_ready = seed_state.entry_confirmation_ready
        fast_entry_confirmation_ready = seed_state.fast_entry_confirmation_ready
        elite_pattern_rank = seed_state.elite_pattern_rank
        target_efficiency_score = seed_state.target_efficiency_score
        extension_score = seed_state.extension_score
        distance_key = "distance_to_support" if direction == "BUY" else "distance_to_resistance"
        distance = float(structure.get(distance_key, 0.02) or 0.02)
        opposing_distance_key = "distance_to_resistance" if direction == "BUY" else "distance_to_support"
        opposing_distance = float(structure.get(opposing_distance_key, 0.02) or 0.02)
        directional_pullback = pullback_score if direction == "BUY" else -pullback_score
        direction_sign = 1 if direction == "BUY" else -1
        neutral_pattern_pullback_ready = seed_state.neutral_pattern_pullback_ready
        if structure_bias not in {"buy", "sell"} and not neutral_pattern_pullback_ready:
            return None
        aligned_trends = sum(
            1
            for state in (trend_15m, trend_1h)
            if self._trend_sign(state) == direction_sign
        )
        required_trends = max(1, int(plan.min_trend_agreement or 0))
        if neutral_pattern_pullback_ready:
            required_trends = min(required_trends, 1)

        pullback_floor = 0.08 if neutral_pattern_pullback_ready else 0.12
        if directional_pullback <= pullback_floor:
            return None

        if direction == "BUY":
            if aligned_trends < required_trends:
                return None
            if breakout_score <= (-0.18 if neutral_pattern_pullback_ready else -0.10):
                return None
            if upside_exhaustion_score >= (0.62 if neutral_pattern_pullback_ready else 0.54):
                return None
        else:
            if aligned_trends < required_trends:
                return None
            if breakout_score >= (0.18 if neutral_pattern_pullback_ready else 0.10):
                return None
            if downside_exhaustion_score >= (0.62 if neutral_pattern_pullback_ready else 0.54):
                return None

        close = frame["close"].astype(float)
        fast = float(close.tail(8).mean())
        slow = float(close.tail(21).mean())
        trend_confirm = 1.0 if ((direction == "BUY" and fast >= slow) or (direction == "SELL" and fast <= slow)) else 0.0
        level_proximity = _clip(1.0 - distance / 0.01)
        if opposing_distance <= max(distance * 0.35, 0.0007):
            return None
        regime_component = 0.74 if (
            (direction == "BUY" and regime == "trending_up")
            or (direction == "SELL" and regime == "trending_down")
        ) else 0.52
        context_confluence = _context_directional_confluence(context, direction)
        confluence_score = _safe_float(context_confluence.get("score"), 0.0)
        support_components = int(context_confluence.get("support_components", 0) or 0)
        conflict_components = int(context_confluence.get("conflict_components", 0) or 0)
        effective_alignment_score = alignment_score
        effective_setup_quality = setup_quality
        if neutral_pattern_pullback_ready:
            effective_alignment_score = max(
                effective_alignment_score,
                0.24 + _clip(elite_pattern_rank) * 0.28 + max(0.0, directional_pullback) * 0.18,
            )
            effective_setup_quality = max(
                effective_setup_quality,
                0.24
                + _clip(elite_pattern_rank) * 0.24
                + max(0.0, directional_pullback) * 0.18
                + (0.08 if first_pullback_ready else 0.0)
                + (0.06 if entry_confirmation_ready or fast_entry_confirmation_ready else 0.0),
            )

        score = (
            _clip(directional_pullback) * 0.30
            + _clip(effective_setup_quality) * 0.20
            + _clip(effective_alignment_score) * 0.18
            + level_proximity * 0.18
            + trend_confirm * 0.14
            + (_clip(elite_pattern_rank) * 0.10 if neutral_pattern_pullback_ready else 0.0)
            + (0.06 if first_pullback_ready else 0.0)
            + (0.05 if entry_confirmation_ready or fast_entry_confirmation_ready else 0.0)
            + (max(0.0, confluence_score) * 0.08 if neutral_pattern_pullback_ready else 0.0)
        )
        confidence = _clip(0.40 + score * 0.40 + regime_component * 0.18, 0.0, 0.92)

        score_floor = profile.pullback_min_score
        if neutral_pattern_pullback_ready:
            score_floor = max(0.44, score_floor - 0.12)
        if score < score_floor:
            return None

        notes = [
            "trend_pullback",
            f"session={session}",
            f"pullback={directional_pullback:.2f}",
            f"level_dist={distance:.4f}",
        ]
        if neutral_pattern_pullback_ready:
            notes.extend(
                [
                    "neutral_pattern_pullback_relief",
                    f"ctx={confluence_score:+.2f}",
                    f"support={support_components}",
                    f"conflict={conflict_components}",
                ]
            )
        return {
            "playbook": "trend_pullback",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "pullback_hold",
            "session": session,
            "preferred_interval": preferred_interval,
            "pullback_confirmed": True,
            "management": self._management_template(profile, "trend_pullback", asset=asset, category=category),
            "notes": notes,
        }

    def _early_inflection(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        plan = self._asset_plan(asset, category)
        if "early_inflection" not in set(plan.allowed_playbooks):
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 14), max(12, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None

        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]
        previous = recent.iloc[-2]

        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        prev_close = float(previous["close"])
        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)

        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        upside_exhaustion_score = float(structure.get("upside_exhaustion_score", 0.0) or 0.0)
        downside_exhaustion_score = float(structure.get("downside_exhaustion_score", 0.0) or 0.0)
        management = self._management_template(profile, "early_inflection", asset=asset, category=category)
        candidates: List[Dict[str, Any]] = []
        sell_candidate = _build_early_inflection_candidate(
            direction="SELL",
            structure_bias=structure_bias,
            latest_open=latest_open,
            latest_close=latest_close,
            latest_high=latest_high,
            latest_low=latest_low,
            prev_close=prev_close,
            range_high=range_high,
            range_low=range_low,
            atr=atr,
            avg_body=avg_body,
            setup_quality=setup_quality,
            alignment_score=alignment_score,
            regime=regime,
            upside_exhaustion_score=upside_exhaustion_score,
            downside_exhaustion_score=downside_exhaustion_score,
            profile=profile,
            preferred_interval=preferred_interval,
            management=management,
            asset=asset,
            category=category,
            session=session,
        )
        if sell_candidate:
            candidates.append(sell_candidate)
        buy_candidate = _build_early_inflection_candidate(
            direction="BUY",
            structure_bias=structure_bias,
            latest_open=latest_open,
            latest_close=latest_close,
            latest_high=latest_high,
            latest_low=latest_low,
            prev_close=prev_close,
            range_high=range_high,
            range_low=range_low,
            atr=atr,
            avg_body=avg_body,
            setup_quality=setup_quality,
            alignment_score=alignment_score,
            regime=regime,
            upside_exhaustion_score=upside_exhaustion_score,
            downside_exhaustion_score=downside_exhaustion_score,
            profile=profile,
            preferred_interval=preferred_interval,
            management=management,
            asset=asset,
            category=category,
            session=session,
        )
        if buy_candidate:
            candidates.append(buy_candidate)
        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _reversal_exhaustion(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 14), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        tolerance = max(atr * 0.12, abs(range_high - range_low) * 0.06, 1e-9)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()

        def _candidate(direction: str, sweep_size: float, reclaim_dist: float, body_strength: float) -> Dict[str, Any]:
            confluence = _context_directional_confluence(context, direction)
            confluence_score = _safe_float(confluence.get("score"), 0.0)
            support_components = int(confluence.get("support_components", 0) or 0)
            conflict_components = int(confluence.get("conflict_components", 0) or 0)
            stretch_component = 0.10 if (
                (direction == "SELL" and structure_bias == "buy")
                or (direction == "BUY" and structure_bias == "sell")
            ) else 0.05
            regime_component = 0.10 if (
                (direction == "SELL" and regime in {"trending_up", "volatile"})
                or (direction == "BUY" and regime in {"trending_down", "volatile"})
            ) else 0.04
            score = (
                _clip(sweep_size / max(atr, 1e-9)) * 0.26
                + _clip(reclaim_dist / max(atr, 1e-9)) * 0.24
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.18
                + _clip(setup_quality) * 0.12
                + _clip(abs(breakout_score)) * 0.10
                + _clip(alignment_score) * 0.10
                + stretch_component
                + regime_component
                + max(0.0, confluence_score) * 0.16
                + min(0.08, support_components * 0.03)
                - max(0.0, -confluence_score) * 0.12
                - min(0.06, conflict_components * 0.02)
            )
            confidence = _clip(0.42 + score * 0.42 + max(0.0, confluence_score) * 0.10, 0.0, 0.95)
            return {
                "playbook": "reversal_exhaustion",
                "direction": direction,
                "score": round(score, 4),
                "confidence": round(confidence, 4),
                "context_confluence": round(confluence_score, 4),
                "cross_alignment": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                "cross_context_support": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                "cross_confidence": round(_safe_float(confluence.get("cross_confidence"), 0.0), 4),
                "micro_score": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                "micro_context_support": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                "whale_context_support": round(_safe_float(confluence.get("whale_support"), 0.0), 4),
                "support_components": support_components,
                "conflict_components": conflict_components,
                "entry_style": "reclaim_reversal",
                "session": session,
                "preferred_interval": preferred_interval,
                "management": self._management_template(profile, "reversal_exhaustion", asset=asset, category=category),
                "notes": [
                    "liquidity_sweep",
                    "reversal_exhaustion",
                    "bearish_reclaim_failure" if direction == "SELL" else "bullish_reclaim_failure",
                    f"ctx={confluence_score:+.2f}",
                    f"session={session}",
                ],
            }

        candidates: List[Dict[str, Any]] = []
        if latest_high >= range_high + tolerance and latest_close <= range_high and latest_close < latest_open:
            sweep_size = latest_high - range_high
            reclaim_dist = range_high - latest_close
            body_strength = latest_open - latest_close
            candidate = _candidate("SELL", sweep_size, reclaim_dist, body_strength)
            if float(candidate["score"]) >= profile.reversal_min_score:
                candidates.append(candidate)

        if latest_low <= range_low - tolerance and latest_close >= range_low and latest_close > latest_open:
            sweep_size = range_low - latest_low
            reclaim_dist = latest_close - range_low
            body_strength = latest_close - latest_open
            candidate = _candidate("BUY", sweep_size, reclaim_dist, body_strength)
            if float(candidate["score"]) >= profile.reversal_min_score:
                candidates.append(candidate)

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _failed_break_reclaim(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 14), max(10, len(frame) - 2))
        recent = frame.tail(lookback + 2)
        if len(recent) < 12:
            return None
        base = recent.iloc[:-2]
        prior_bar = recent.iloc[-2]
        latest = recent.iloc[-1]
        if base.empty:
            return None

        range_high = float(base["high"].max())
        range_low = float(base["low"].min())
        atr = self._atr(recent.tail(24))
        avg_body = float((base["close"] - base["open"]).abs().tail(lookback).mean() or 0.0)
        tolerance = max(atr * 0.10, abs(range_high - range_low) * 0.05, 1e-9)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        prior_close = float(prior_bar["close"])
        prior_high = float(prior_bar["high"])
        prior_low = float(prior_bar["low"])
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])

        candidates: List[Dict[str, Any]] = []
        if prior_close >= range_high + tolerance and latest_close <= range_high and latest_close < latest_open:
            reclaim = range_high - latest_close
            body_strength = latest_open - latest_close
            lower_high = max(0.0, prior_high - latest_high)
            confluence = _context_directional_confluence(context, "SELL")
            confluence_score = _safe_float(confluence.get("score"), 0.0)
            support_components = int(confluence.get("support_components", 0) or 0)
            conflict_components = int(confluence.get("conflict_components", 0) or 0)
            score = (
                _clip(reclaim / max(atr, 1e-9)) * 0.30
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.20
                + _clip(lower_high / max(atr, 1e-9)) * 0.14
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
                + max(0.0, confluence_score) * 0.18
                + min(0.08, support_components * 0.03)
                - max(0.0, -confluence_score) * 0.12
                - min(0.06, conflict_components * 0.02)
            )
            confidence = _clip(0.42 + score * 0.42 + max(0.0, confluence_score) * 0.10, 0.0, 0.94)
            if score >= profile.reversal_min_score:
                candidates.append(
                    {
                        "playbook": "failed_break_reclaim",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "context_confluence": round(confluence_score, 4),
                        "cross_alignment": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                        "cross_context_support": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                        "cross_confidence": round(_safe_float(confluence.get("cross_confidence"), 0.0), 4),
                        "micro_score": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                        "micro_context_support": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                        "whale_context_support": round(_safe_float(confluence.get("whale_support"), 0.0), 4),
                        "support_components": support_components,
                        "conflict_components": conflict_components,
                        "reclaim_confirmed": True,
                        "entry_style": "reclaim_failure",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "failed_break_reclaim", asset=asset, category=category),
                        "notes": [
                            "failed_breakout",
                            "lower_high",
                            "bearish_reclaim_failure",
                            f"ctx={confluence_score:+.2f}",
                            f"session={session}",
                        ],
                    }
                )

        if prior_close <= range_low - tolerance and latest_close >= range_low and latest_close > latest_open:
            reclaim = latest_close - range_low
            body_strength = latest_close - latest_open
            higher_low = max(0.0, latest_low - prior_low)
            confluence = _context_directional_confluence(context, "BUY")
            confluence_score = _safe_float(confluence.get("score"), 0.0)
            support_components = int(confluence.get("support_components", 0) or 0)
            conflict_components = int(confluence.get("conflict_components", 0) or 0)
            score = (
                _clip(reclaim / max(atr, 1e-9)) * 0.30
                + _clip(body_strength / max(avg_body * 2.0, 1e-9)) * 0.20
                + _clip(higher_low / max(atr, 1e-9)) * 0.14
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
                + max(0.0, confluence_score) * 0.18
                + min(0.08, support_components * 0.03)
                - max(0.0, -confluence_score) * 0.12
                - min(0.06, conflict_components * 0.02)
            )
            confidence = _clip(0.42 + score * 0.42 + max(0.0, confluence_score) * 0.10, 0.0, 0.94)
            if score >= profile.reversal_min_score:
                candidates.append(
                    {
                        "playbook": "failed_break_reclaim",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "context_confluence": round(confluence_score, 4),
                        "cross_alignment": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                        "cross_context_support": round(_safe_float(confluence.get("cross_support"), 0.0), 4),
                        "cross_confidence": round(_safe_float(confluence.get("cross_confidence"), 0.0), 4),
                        "micro_score": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                        "micro_context_support": round(_safe_float(confluence.get("micro_support"), 0.0), 4),
                        "whale_context_support": round(_safe_float(confluence.get("whale_support"), 0.0), 4),
                        "support_components": support_components,
                        "conflict_components": conflict_components,
                        "reclaim_confirmed": True,
                        "entry_style": "reclaim_failure",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "failed_break_reclaim", asset=asset, category=category),
                        "notes": [
                            "failed_breakout",
                            "higher_low",
                            "bullish_reclaim_failure",
                            f"ctx={confluence_score:+.2f}",
                            f"session={session}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _aggressive_expansion_trigger(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 16), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        previous_high = float(prior["high"].tail(min(12, len(prior))).max())
        previous_low = float(prior["low"].tail(min(12, len(prior))).min())
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        body = abs(latest_close - latest_open)
        close_near_low = _clip((latest_high - latest_close) / max(atr * 0.8, 1e-9), 0.0, 1.0)
        close_near_high = _clip((latest_close - latest_low) / max(atr * 0.8, 1e-9), 0.0, 1.0)

        if latest_close < latest_open and latest_close <= previous_low:
            expansion_dist = previous_low - latest_close
            score = (
                _clip(body / max(avg_body * 2.5, 1e-9)) * 0.34
                + _clip(expansion_dist / max(atr, 1e-9)) * 0.26
                + close_near_low * 0.10
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.08
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
            )
            confidence = _clip(0.40 + score * 0.44, 0.0, 0.94)
            if score >= profile.expansion_min_score:
                return {
                    "playbook": "aggressive_expansion",
                    "direction": "SELL",
                    "score": round(score, 4),
                    "confidence": round(confidence, 4),
                    "entry_style": "expansion_break",
                    "session": session,
                    "preferred_interval": preferred_interval,
                    "management": self._management_template(profile, "aggressive_expansion", asset=asset, category=category),
                    "notes": [
                        "aggressive_downside_expansion",
                        f"session={session}",
                        f"body_x={body / max(avg_body, 1e-9):.2f}",
                    ],
                }

        if latest_close > latest_open and latest_close >= previous_high:
            expansion_dist = latest_close - previous_high
            score = (
                _clip(body / max(avg_body * 2.5, 1e-9)) * 0.34
                + _clip(expansion_dist / max(atr, 1e-9)) * 0.26
                + close_near_high * 0.10
                + _clip(setup_quality) * 0.16
                + _clip(alignment_score) * 0.08
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
            )
            confidence = _clip(0.40 + score * 0.44, 0.0, 0.94)
            if score >= profile.expansion_min_score:
                return {
                    "playbook": "aggressive_expansion",
                    "direction": "BUY",
                    "score": round(score, 4),
                    "confidence": round(confidence, 4),
                    "entry_style": "expansion_break",
                    "session": session,
                    "preferred_interval": preferred_interval,
                    "management": self._management_template(profile, "aggressive_expansion", asset=asset, category=category),
                    "notes": [
                        "aggressive_upside_expansion",
                        f"session={session}",
                        f"body_x={body / max(avg_body, 1e-9):.2f}",
                    ],
                }
        return None

    def _opening_drive(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category not in {"indices", "commodities"}:
            return None
        if not str(session or "").endswith("_open"):
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 12), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 12:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]

        range_high = float(prior["high"].max())
        range_low = float(prior["low"].min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        latest_high = float(latest["high"])
        latest_low = float(latest["low"])
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        body = abs(latest_close - latest_open)
        if body <= 0.0:
            return None

        candidates: List[Dict[str, Any]] = []
        if latest_close > range_high and latest_close > latest_open:
            impulse = latest_close - range_high
            close_strength = _clip((latest_close - latest_low) / max(atr * 0.8, 1e-9))
            score = (
                _clip(body / max(avg_body * 2.1, 1e-9)) * 0.30
                + _clip(impulse / max(atr, 1e-9)) * 0.24
                + close_strength * 0.12
                + _clip(setup_quality) * 0.14
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_up", "volatile"} else 0.04)
            )
            confidence = _clip(0.43 + score * 0.43, 0.0, 0.95)
            if score >= profile.breakout_min_score:
                candidates.append(
                    {
                        "playbook": "opening_drive",
                        "direction": "BUY",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "opening_drive_break",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "opening_drive", asset=asset, category=category),
                        "notes": [
                            "opening_drive",
                            "cash_open_break",
                            f"session={session}",
                        ],
                    }
                )

        if latest_close < range_low and latest_close < latest_open:
            impulse = range_low - latest_close
            close_strength = _clip((latest_high - latest_close) / max(atr * 0.8, 1e-9))
            score = (
                _clip(body / max(avg_body * 2.1, 1e-9)) * 0.30
                + _clip(impulse / max(atr, 1e-9)) * 0.24
                + close_strength * 0.12
                + _clip(setup_quality) * 0.14
                + _clip(alignment_score) * 0.10
                + (0.10 if regime in {"trending_down", "volatile"} else 0.04)
            )
            confidence = _clip(0.43 + score * 0.43, 0.0, 0.95)
            if score >= profile.breakout_min_score:
                candidates.append(
                    {
                        "playbook": "opening_drive",
                        "direction": "SELL",
                        "score": round(score, 4),
                        "confidence": round(confidence, 4),
                        "entry_style": "opening_drive_break",
                        "session": session,
                        "preferred_interval": preferred_interval,
                        "management": self._management_template(profile, "opening_drive", asset=asset, category=category),
                        "notes": [
                            "opening_drive",
                            "cash_open_break",
                            f"session={session}",
                        ],
                    }
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        return candidates[0]

    def _news_impulse(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category not in {"forex", "commodities"}:
            return None

        news = dict((context or {}).get("news_event") or {})
        news_state = str(news.get("state") or "").strip().lower()
        impact = str(news.get("impact") or "").strip().upper()
        direction_sign = _news_direction_sign(news.get("direction"))
        if news_state not in {"active", "post"} or impact not in {"HIGH", "MEDIUM"} or direction_sign == 0:
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 10), max(8, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 10:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        body = abs(float(latest["close"]) - float(latest["open"]))
        prior_high = float(prior["high"].max())
        prior_low = float(prior["low"].min())
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        regime = str(structure.get("regime", "unknown") or "unknown").lower()

        if direction_sign > 0:
            if float(latest["close"]) <= max(prior_high, float(latest["open"])):
                return None
            impulse = float(latest["close"]) - prior_high
            close_strength = _clip((float(latest["close"]) - float(latest["low"])) / max(atr * 0.8, 1e-9))
            direction = "BUY"
        else:
            if float(latest["close"]) >= min(prior_low, float(latest["open"])):
                return None
            impulse = prior_low - float(latest["close"])
            close_strength = _clip((float(latest["high"]) - float(latest["close"])) / max(atr * 0.8, 1e-9))
            direction = "SELL"

        impact_bonus = 0.10 if impact == "HIGH" else 0.05
        state_bonus = 0.08 if news_state == "active" else 0.05
        regime_bonus = 0.08 if (
            (direction == "BUY" and regime in {"trending_up", "volatile"})
            or (direction == "SELL" and regime in {"trending_down", "volatile"})
        ) else 0.03
        score = (
            _clip(body / max(avg_body * 2.4, 1e-9)) * 0.28
            + _clip(impulse / max(atr, 1e-9)) * 0.24
            + close_strength * 0.10
            + _clip(setup_quality) * 0.14
            + _clip(alignment_score) * 0.09
            + impact_bonus
            + state_bonus
            + regime_bonus
        )
        confidence = _clip(0.44 + score * 0.42, 0.0, 0.95)
        if score < max(profile.breakout_min_score, 0.60):
            return None
        return {
            "playbook": "news_impulse",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "entry_style": "news_followthrough",
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "news_impulse", asset=asset, category=category),
            "notes": [
                "news_impulse",
                f"impact={impact}",
                f"event={str(news.get('event') or 'macro')[:36]}",
                f"session={session}",
            ],
        }

    def _intermarket_continuation(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category not in {"forex", "indices", "commodities"}:
            return None

        cross = dict((context or {}).get("cross_asset_context") or {})
        micro = dict((context or {}).get("market_microstructure") or {})
        if not cross and not micro:
            return None

        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        if structure_bias == "buy":
            direction = "BUY"
        elif structure_bias == "sell":
            direction = "SELL"
        else:
            direction = str(cross.get("supportive_direction") or "").strip().upper()
            if direction not in {"BUY", "SELL"}:
                return None

        direction_sign = _playbook_direction_sign(direction)
        plan = self._asset_plan(asset, category)
        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)

        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        pullback_score = float(structure.get("pullback_score", 0.0) or 0.0)
        candle_quality_score = float(structure.get("candle_quality_score", 0.0) or 0.0)
        session_quality_score = float(structure.get("session_quality_score", 0.0) or 0.0)
        target_efficiency_score = float(structure.get("target_efficiency_score", 0.0) or 0.0)
        volatility_state = str(structure.get("volatility_state", "unknown") or "unknown").lower()
        regime = str(structure.get("regime", "unknown") or "unknown").lower()
        pattern_family = str(structure.get("pattern_family", "unknown") or "unknown").lower()

        directional_breakout = breakout_score if direction == "BUY" else -breakout_score
        directional_pullback = pullback_score if direction == "BUY" else -pullback_score

        cross_alignment = _safe_float(cross.get("alignment", cross.get("score", 0.0)), 0.0) * direction_sign
        cross_confidence = _safe_float(cross.get("confidence", 0.0), 0.0)
        dominant_peer = str(cross.get("dominant_peer") or "")
        dominant_relation = str(cross.get("dominant_relation") or "")
        cross_support = _clip(cross_alignment)

        micro_score = _safe_float(micro.get("score", 0.0), 0.0) * direction_sign
        trade_flow_support = _safe_float(micro.get("trade_flow_score", micro.get("score", 0.0)), 0.0) * direction_sign
        book_support = _safe_float(micro.get("book_imbalance", 0.0), 0.0) * direction_sign
        tick_support = _safe_float(micro.get("tick_imbalance", 0.0), 0.0) * direction_sign
        velocity_support = _clip((_safe_float(micro.get("velocity_bps", 0.0), 0.0) * direction_sign) / 4.0)
        micro_support = _clip(max(micro_score, trade_flow_support, book_support * 0.9, tick_support * 0.75, velocity_support))
        spread_bps = _safe_float(micro.get("spread_bps", 0.0), 0.0)

        if cross_support < 0.18 and micro_support < 0.18:
            return None
        if cross_confidence < 0.28 and cross_support < 0.22 and micro_support < 0.22:
            return None
        spread_limit = 40.0 if category == "indices" else 24.0 if category == "commodities" else 18.0
        if spread_bps > spread_limit:
            return None

        if alignment_score < max(0.45, float(plan.min_alignment_score) - 0.12) and cross_support < 0.32:
            return None
        if setup_quality < max(0.42, float(plan.min_setup_quality) - 0.12) and micro_support < 0.24:
            return None

        breakout_retest_ready = bool(structure.get("breakout_retest_ready"))
        first_pullback_ready = bool(structure.get("first_pullback_ready"))
        entry_confirmation_ready = bool(structure.get("entry_confirmation_ready"))
        readiness_score = 0.0
        entry_style = "intermarket_continuation"
        if breakout_retest_ready and directional_breakout >= 0.15:
            readiness_score = 0.16
            entry_style = "intermarket_retest"
        elif first_pullback_ready and directional_pullback >= 0.15:
            readiness_score = 0.16
            entry_style = "intermarket_pullback"
        elif entry_confirmation_ready and directional_breakout >= 0.22:
            readiness_score = 0.12
            entry_style = "intermarket_confirmed_break"
        elif directional_breakout >= 0.34:
            readiness_score = 0.10
            entry_style = "intermarket_break"
        elif directional_pullback >= 0.26:
            readiness_score = 0.08
            entry_style = "intermarket_trend_hold"

        if readiness_score <= 0.0 and cross_support < 0.30 and micro_support < 0.25:
            return None

        score = _clip(
            cross_support * 0.22
            + _clip(cross_confidence) * 0.16
            + micro_support * 0.18
            + _clip(max(directional_breakout, directional_pullback)) * 0.14
            + _clip(setup_quality) * 0.12
            + _clip(alignment_score) * 0.08
            + _clip(candle_quality_score) * 0.04
            + _clip(session_quality_score) * 0.03
            + _clip(target_efficiency_score) * 0.03
            + readiness_score
            + (0.04 if category == "indices" and str(session or "").endswith("_open") else 0.0)
            + (0.04 if category == "commodities" and dominant_relation in {"silver_confirmation", "gold_lead", "cad_confirmation", "growth_cycle_confirmation"} else 0.0)
            + (0.03 if volatility_state in {"expansion", "normal"} else 0.0)
            + (0.03 if pattern_family.startswith("trending_") else 0.0)
            + (0.02 if (direction == "BUY" and regime in {"trending_up", "volatile"}) or (direction == "SELL" and regime in {"trending_down", "volatile"}) else 0.0),
            0.0,
            1.0,
        )
        if score < max(profile.breakout_min_score, 0.58):
            return None

        confidence = _clip(0.43 + score * 0.42 + min(0.05, cross_confidence * 0.06), 0.0, 0.95)
        notes = [
            "intermarket_continuation",
            f"cross={cross_alignment:+.2f}/{cross_confidence:.2f}",
            f"micro={micro_support:.2f}",
            f"session={session}",
        ]
        if dominant_peer:
            notes.append(f"peer={dominant_peer}")
        if dominant_relation:
            notes.append(f"relation={dominant_relation}")

        return {
            "playbook": "intermarket_continuation",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "cross_alignment": round(cross_alignment, 4),
            "cross_confidence": round(cross_confidence, 4),
            "micro_score": round(micro_support, 4),
            "entry_style": entry_style,
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "intermarket_continuation", asset=asset, category=category),
            "notes": notes,
        }

    def _crypto_orderflow_continuation(
        self,
        frame: pd.DataFrame,
        *,
        asset: str,
        structure: Dict[str, Any],
        category: str,
        session: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        if category != "crypto":
            return None

        micro = dict((context or {}).get("market_microstructure") or {})
        true_depth = bool(micro.get("depth_available"))
        synthetic_depth = bool(micro.get("synthetic_depth_available"))
        if not true_depth and not synthetic_depth:
            return None

        imbalance = _safe_float(micro.get("book_imbalance", micro.get("score", 0.0)), 0.0)
        micro_score = _safe_float(micro.get("score", 0.0), 0.0)
        spread_bps = _safe_float(micro.get("spread_bps", 0.0), 0.0)
        threshold = 0.18 if true_depth else 0.26
        if abs(imbalance) < threshold or spread_bps > 35.0:
            return None

        profile = self._profile(category)
        preferred_interval = self.preferred_interval(category, asset)
        lookback = min(max(profile.breakout_lookback, 12), max(10, len(frame) - 1))
        recent = frame.tail(lookback + 1)
        if len(recent) < 10:
            return None
        prior = recent.iloc[:-1]
        latest = recent.iloc[-1]
        atr = self._atr(recent.tail(24))
        avg_body = float((prior["close"] - prior["open"]).abs().tail(lookback).mean() or 0.0)
        prev_high = float(prior["high"].tail(min(12, len(prior))).max())
        prev_low = float(prior["low"].tail(min(12, len(prior))).min())
        latest_open = float(latest["open"])
        latest_close = float(latest["close"])
        body = abs(latest_close - latest_open)
        setup_quality = float(structure.get("setup_quality", 0.0) or 0.0)
        alignment_score = float(structure.get("alignment_score", 0.0) or 0.0)
        structure_bias = str(structure.get("structure_bias", "neutral") or "neutral").lower()
        breakout_score = float(structure.get("breakout_score", 0.0) or 0.0)
        trend_15m = str(structure.get("trend_15m", "unknown") or "unknown").lower()
        trend_1h = str(structure.get("trend_1h", "unknown") or "unknown").lower()
        entry_style = "orderflow_break"
        directional_trend_ready = False
        recent_mean = float(prior["close"].tail(min(6, len(prior))).mean() or 0.0)
        recent_band_high = float(prior["high"].tail(min(6, len(prior))).max() or prev_high)
        recent_band_low = float(prior["low"].tail(min(6, len(prior))).min() or prev_low)
        followthrough_tolerance = max(atr * 0.18, abs(prev_high - prev_low) * 0.04, 1e-9)
        down_trend_aligned = all(self._trend_sign(state) <= 0 for state in (trend_15m, trend_1h))
        up_trend_aligned = all(self._trend_sign(state) >= 0 for state in (trend_15m, trend_1h))
        strong_pressure_override = bool(
            abs(imbalance) >= (0.52 if true_depth else 0.62)
            and abs(micro_score) >= 0.34
            and body >= max(avg_body * 0.55, atr * 0.16, 1e-9)
            and spread_bps <= 18.0
        )
        direction = ""
        impulse = 0.0

        if imbalance > 0 and latest_close > prev_high and latest_close > latest_open:
            impulse = latest_close - prev_high
            direction = "BUY"
            if (
                impulse <= followthrough_tolerance
                and structure_bias == "buy"
                and alignment_score >= 0.72
                and setup_quality >= 0.54
                and latest_close >= recent_mean
                and up_trend_aligned
            ):
                directional_trend_ready = True
                entry_style = "orderflow_followthrough"
        elif imbalance < 0 and latest_close < prev_low and latest_close < latest_open:
            impulse = prev_low - latest_close
            direction = "SELL"
            if (
                impulse <= followthrough_tolerance
                and structure_bias == "sell"
                and alignment_score >= 0.72
                and setup_quality >= 0.54
                and latest_close <= recent_mean
                and down_trend_aligned
            ):
                directional_trend_ready = True
                entry_style = "orderflow_followthrough"
        elif (
            imbalance > 0
            and structure_bias == "buy"
            and alignment_score >= 0.72
            and setup_quality >= 0.54
            and latest_close > latest_open
            and latest_close >= recent_mean
            and latest_close >= recent_band_high - followthrough_tolerance
            and up_trend_aligned
            and breakout_score >= -0.08
        ):
            impulse = max(latest_close - recent_mean, latest_close - float(prior["close"].iloc[-1]), 0.0)
            direction = "BUY"
            directional_trend_ready = True
            entry_style = "orderflow_followthrough"
        elif (
            imbalance < 0
            and structure_bias == "sell"
            and alignment_score >= 0.72
            and setup_quality >= 0.54
            and latest_close < latest_open
            and latest_close <= recent_mean
            and latest_close <= recent_band_low + followthrough_tolerance
            and down_trend_aligned
            and breakout_score <= 0.08
        ):
            impulse = max(recent_mean - latest_close, float(prior["close"].iloc[-1]) - latest_close, 0.0)
            direction = "SELL"
            directional_trend_ready = True
            entry_style = "orderflow_followthrough"
        else:
            return None

        if (
            direction == "BUY"
            and not directional_trend_ready
            and strong_pressure_override
            and latest_close > latest_open
            and latest_close >= recent_mean
            and latest_close >= recent_band_high - followthrough_tolerance
            and breakout_score >= -0.16
        ):
            impulse = max(impulse, latest_close - recent_mean, latest_close - float(prior["close"].iloc[-1]))
            directional_trend_ready = True
            entry_style = "orderflow_pressure_followthrough"
        elif (
            direction == "SELL"
            and not directional_trend_ready
            and strong_pressure_override
            and latest_close < latest_open
            and latest_close <= recent_mean
            and latest_close <= recent_band_low + followthrough_tolerance
            and breakout_score <= 0.16
        ):
            impulse = max(impulse, recent_mean - latest_close, float(prior["close"].iloc[-1]) - latest_close)
            directional_trend_ready = True
            entry_style = "orderflow_pressure_followthrough"

        score = (
            _clip(abs(imbalance)) * 0.22
            + _clip(abs(micro_score)) * 0.16
            + _clip(body / max(avg_body * 2.1, 1e-9)) * 0.18
            + _clip(impulse / max(atr, 1e-9)) * 0.18
            + _clip(setup_quality) * 0.12
            + _clip(alignment_score) * 0.08
            + (0.06 if true_depth else 0.02)
            + (0.03 if directional_trend_ready else 0.0)
            + (0.04 if entry_style == "orderflow_pressure_followthrough" else 0.0)
        )
        confidence = _clip(0.43 + score * 0.43, 0.0, 0.96)
        min_score_floor = max(profile.breakout_min_score, 0.60)
        if directional_trend_ready:
            min_score_floor = max(0.52, min_score_floor - 0.08)
        if score < min_score_floor:
            return None

        return {
            "playbook": "crypto_orderflow_continuation",
            "direction": direction,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "book_imbalance": round(imbalance, 4),
            "micro_score": round(micro_score, 4),
            "spread_bps": round(spread_bps, 2),
            "entry_style": entry_style,
            "session": session,
            "preferred_interval": preferred_interval,
            "management": self._management_template(profile, "crypto_orderflow_continuation", asset=asset, category=category),
            "notes": [
                "crypto_orderflow_continuation",
                "true_depth" if true_depth else "synthetic_depth",
                f"imbalance={imbalance:.2f}",
                f"spread_bps={spread_bps:.1f}",
                "followthrough" if directional_trend_ready else "fresh_break",
                f"session={session}",
            ],
        }

    def analyze(
        self,
        asset: str,
        category: str,
        price_data,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context = context or {}
        frame = self._frame(price_data)
        if frame is None:
            return {"asset": asset, "category": category, "candidates": [], "primary": None}

        plan = self._asset_plan(asset, category)
        session_allowed, session, allowed_sessions = self._session_allowed(asset, category)
        inactivity_profile = _merge_context_relief_profiles(
            _context_inactivity_profile(context),
            _context_participation_relief(
                context,
                asset=asset,
                category=category,
            ),
        )
        if not session_allowed:
            return {
                "asset": asset,
                "category": category,
                "candidates": [],
                "primary": None,
                "blocked_reason": f"session_block:{session}",
                "session": session,
                "allowed_sessions": list(allowed_sessions),
                "inactivity_profile": dict(inactivity_profile),
                "asset_plan": {
                    "allowed_playbooks": list(plan.allowed_playbooks),
                    "allowed_sessions": list(allowed_sessions),
                },
            }

        structure = dict(context.get("market_structure") or {})
        candidates: List[Dict[str, Any]] = []
        rejected_reasons: List[str] = []
        rejected_details: List[str] = []
        rejected_records: List[Dict[str, Any]] = []
        for builder in (
            self._news_impulse,
            self._opening_drive,
            self._intermarket_continuation,
            self._crypto_orderflow_continuation,
            self._early_inflection,
            self._reversal_exhaustion,
            self._failed_break_reclaim,
            self._aggressive_expansion_trigger,
            self._breakout_continuation,
            self._breakout_retest,
            self._trend_pullback,
        ):
            candidate = builder(
                frame,
                asset=asset,
                structure=structure,
                category=category,
                session=session,
                context=context,
            )
            if candidate:
                approved, reason = self._qualify_candidate(
                    candidate,
                    asset=asset,
                    category=category,
                    structure=structure,
                    plan=plan,
                    inactivity_profile=inactivity_profile,
                    context=context,
                )
                if approved:
                    candidates.append(candidate)
                elif reason:
                    rejected_reasons.append(reason)
                    detail = candidate.get("qualification") or {}
                    rejected_records.append(
                        {
                            "reason": reason,
                            "score": _safe_float(detail.get("score", candidate.get("score", 0.0)), 0.0),
                            "confidence": _safe_float(detail.get("confidence", candidate.get("confidence", 0.0)), 0.0),
                            "playbook": detail.get("playbook", candidate.get("playbook", "unknown")),
                        }
                    )
                    rejected_details.append(
                        (
                            f"{detail.get('playbook', candidate.get('playbook', 'unknown'))}:{detail.get('direction', candidate.get('direction', ''))}"
                            f":reason={reason}"
                            f":score={_safe_float(detail.get('score', candidate.get('score', 0.0)), 0.0):.3f}"
                            f":align={_safe_float(detail.get('alignment_score', 0.0), 0.0):.3f}"
                            f":setup={_safe_float(detail.get('setup_quality', 0.0), 0.0):.3f}"
                            f":trends={int(detail.get('aligned_trends', 0) or 0)}/{int(detail.get('effective_required_trends', detail.get('required_trends', 0)) or 0)}"
                            f":early={int(bool(detail.get('allow_early_trend_relief')))}"
                            f":strong={int(bool(detail.get('strong_impulse_break')))}"
                            f":ctx={_safe_float(detail.get('context_confluence', 0.0), 0.0):.3f}"
                            f":support={int(detail.get('support_components', 0) or 0)}"
                            f":conflict={int(detail.get('conflict_components', 0) or 0)}"
                            f":family={str(detail.get('pattern_family', 'unknown') or 'unknown')}"
                            f":confirm={int(detail.get('entry_confirmation_count', 0) or 0)}/{int(detail.get('entry_confirmation_bars_required', 0) or 0)}"
                        )
                    )

        if not candidates:
            fallback = self._elite_ready_fallback(
                asset=asset,
                category=category,
                session=session,
                structure=structure,
                plan=plan,
                inactivity_profile=inactivity_profile,
                context=context,
            )
            if fallback:
                approved, reason = self._qualify_candidate(
                    fallback,
                    asset=asset,
                    category=category,
                    structure=structure,
                    plan=plan,
                    inactivity_profile=inactivity_profile,
                    context=context,
                )
                if approved:
                    candidates.append(fallback)
                elif reason:
                    rejected_reasons.append(reason)
                    detail = fallback.get("qualification") or {}
                    rejected_records.append(
                        {
                            "reason": reason,
                            "score": _safe_float(detail.get("score", fallback.get("score", 0.0)), 0.0),
                            "confidence": _safe_float(detail.get("confidence", fallback.get("confidence", 0.0)), 0.0),
                            "playbook": detail.get("playbook", fallback.get("playbook", "unknown")),
                        }
                    )
                    rejected_details.append(
                        (
                            f"{detail.get('playbook', fallback.get('playbook', 'unknown'))}:{detail.get('direction', fallback.get('direction', ''))}"
                            f":reason={reason}"
                            f":score={_safe_float(detail.get('score', fallback.get('score', 0.0)), 0.0):.3f}"
                            f":align={_safe_float(detail.get('alignment_score', 0.0), 0.0):.3f}"
                            f":setup={_safe_float(detail.get('setup_quality', 0.0), 0.0):.3f}"
                            f":trends={int(detail.get('aligned_trends', 0) or 0)}/{int(detail.get('effective_required_trends', detail.get('required_trends', 0)) or 0)}"
                            f":early={int(bool(detail.get('allow_early_trend_relief')))}"
                            f":strong={int(bool(detail.get('strong_impulse_break')))}"
                            f":ctx={_safe_float(detail.get('context_confluence', 0.0), 0.0):.3f}"
                            f":support={int(detail.get('support_components', 0) or 0)}"
                            f":conflict={int(detail.get('conflict_components', 0) or 0)}"
                            f":family={str(detail.get('pattern_family', 'unknown') or 'unknown')}"
                            f":confirm={int(detail.get('entry_confirmation_count', 0) or 0)}/{int(detail.get('entry_confirmation_bars_required', 0) or 0)}"
                        )
                    )

        candidates.sort(key=lambda item: (float(item.get("confidence", 0.0)), float(item.get("score", 0.0))), reverse=True)
        primary = dict(candidates[0]) if candidates else None
        blocked_reason = "" if primary else _best_rejected_reason(rejected_records)
        if not primary and not blocked_reason:
            blocked_reason = _no_seed_probe_reason(
                category=category,
                structure=structure,
                plan=plan,
                context=context,
            )
        return {
            "asset": asset,
            "category": category,
            "session": session,
            "allowed_sessions": list(allowed_sessions),
            "inactivity_profile": dict(inactivity_profile),
            "candidates": candidates,
            "primary": primary,
            "blocked_reason": blocked_reason,
            "rejected_reasons": rejected_reasons[:5],
            "rejected_details": rejected_details[:5],
            "asset_plan": {
                "allowed_playbooks": list(plan.allowed_playbooks),
                "allowed_sessions": list(allowed_sessions),
                "min_alignment_score": round(float(plan.min_alignment_score), 4),
                "min_setup_quality": round(float(plan.min_setup_quality), 4),
                "min_trend_agreement": int(plan.min_trend_agreement),
            },
        }

    def pick_seed(
        self,
        asset: str,
        category: str,
        price_data,
        context: Optional[Dict[str, Any]] = None,
        *,
        ml_direction: str = "",
        ml_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        analysis = self.analyze(asset, category, price_data, context=context)
        best = analysis.get("primary")
        if not best:
            context_snapshot = _dominant_context_snapshot(context)
            return {
                "action": "",
                "asset": asset,
                "category": category,
                "primary": None,
                "blocked_reason": analysis.get("blocked_reason", ""),
                "session": analysis.get("session", ""),
                "session_label": analysis.get("session", ""),
                "inactivity_profile": dict(analysis.get("inactivity_profile") or {}),
                "rejected_reasons": list(analysis.get("rejected_reasons") or []),
                "rejected_details": list(analysis.get("rejected_details") or []),
                "allowed_sessions": list(analysis.get("allowed_sessions") or []),
                "asset_plan": dict(analysis.get("asset_plan") or {}),
                "context_direction": str(context_snapshot.get("direction") or ""),
                "context_confluence": round(_safe_float(context_snapshot.get("context_confluence"), 0.0), 4),
                "cross_alignment": round(_safe_float(context_snapshot.get("cross_alignment"), 0.0), 4),
                "cross_confidence": round(_safe_float(context_snapshot.get("cross_confidence"), 0.0), 4),
                "micro_score": round(_safe_float(context_snapshot.get("micro_score"), 0.0), 4),
                "whale_context_support": round(_safe_float(context_snapshot.get("whale_context_support"), 0.0), 4),
                "support_components": int(context_snapshot.get("support_components", 0) or 0),
                "conflict_components": int(context_snapshot.get("conflict_components", 0) or 0),
            }

        profile = self._profile(category)
        direction = str(best.get("direction") or "").upper()
        confidence = float(best.get("confidence", 0.0) or 0.0)
        entry_style = str(best.get("entry_style") or "").strip().lower()
        ml_direction = str(ml_direction or "").upper()
        ml_confidence = float(ml_confidence or 0.0)
        action = ""
        seed_floor = float(profile.seed_min_confidence)
        if entry_style == "elite_sweep_continuation":
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.06))
        elif entry_style == "elite_early_continuation":
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.04))
        elif entry_style == "elite_trend_continuation":
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.04))
        elif entry_style == "elite_context_continuation":
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.05))
        elif entry_style == "elite_context_pressure":
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.03))
        elif entry_style in {
            "breakout_close",
            "expansion_break",
            "opening_drive_break",
            "news_followthrough",
            "intermarket_break",
            "intermarket_confirmed_break",
            "intermarket_trend_hold",
            "breakout_ignition",
        }:
            seed_floor = min(seed_floor, max(float(profile.support_min_confidence), float(profile.seed_min_confidence) - 0.03))

        if not ml_direction or ml_confidence < 0.10:
            if confidence >= seed_floor:
                action = "seed"
        elif direction == ml_direction:
            if confidence >= profile.support_min_confidence:
                action = "support"
        elif confidence >= max(profile.override_min_confidence, ml_confidence + profile.override_gap) and ml_confidence <= profile.weak_ml_confidence:
            action = "override"

        return {
            "action": action,
            "asset": asset,
            "category": category,
            "session": analysis.get("session", ""),
            "session_label": analysis.get("session", ""),
            "inactivity_profile": dict(analysis.get("inactivity_profile") or {}),
            "primary": best,
            "candidates": analysis.get("candidates", []),
            "blocked_reason": analysis.get("blocked_reason", ""),
            "rejected_reasons": list(analysis.get("rejected_reasons") or []),
            "rejected_details": list(analysis.get("rejected_details") or []),
            "allowed_sessions": list(analysis.get("allowed_sessions") or []),
            "asset_plan": dict(analysis.get("asset_plan") or {}),
        }


_service = PlaybookService()


def get_service() -> PlaybookService:
    return _service
