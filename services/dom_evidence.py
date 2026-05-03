from __future__ import annotations

from typing import Any, Dict, Mapping


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _tokens(value: Any) -> set[str]:
    return {part.strip().lower() for part in str(value or "").replace("|", ",").split(",") if part.strip()}


def classify_dom_evidence(payload: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    data = dict(payload or {})
    flags = _tokens(data.get("flags"))
    update_mode = str(data.get("depth_update_mode") or "").strip().lower()
    source = str(data.get("microstructure_source") or data.get("depth_provider") or "").strip().lower()

    depth_levels = _safe_int(data.get("depth_levels"), 0)
    snapshot_count = _safe_int(data.get("dom_snapshot_count"), 0)
    delta_count = _safe_int(data.get("dom_delta_count"), 0)
    trade_count = _safe_int(data.get("dom_trade_count"), 0)
    depth_available = bool(data.get("depth_available")) or depth_levels > 0
    synthetic_depth_available = bool(data.get("synthetic_depth_available"))

    stream_health_known = bool(data.get("dom_stream_health_known"))
    stream_health_score = _safe_float(data.get("dom_stream_health_score"), 1.0)
    stream_degraded = bool(data.get("dom_stream_degraded")) or bool(data.get("dom_depth_stream_missing"))

    if not update_mode:
        if "ladder_delta" in flags or "depth_delta" in flags or delta_count > 0:
            update_mode = "event_stream"
        elif "stream_snapshot" in flags:
            update_mode = "stream_snapshot"
        elif "depth_snapshot" in flags or depth_levels > 0:
            update_mode = "snapshot_poll"
        elif synthetic_depth_available:
            update_mode = "synthetic_proxy"
        else:
            update_mode = "none"

    stream_snapshot_ready = bool(data.get("dom_stream_snapshot_ready")) or update_mode == "stream_snapshot"
    event_backed = (
        update_mode == "event_stream"
        or "ladder_delta" in flags
        or "depth_delta" in flags
        or delta_count > 0
        or trade_count > 0
    )
    ladder_ready = bool(depth_available and event_backed and (delta_count > 0 or "ladder_delta" in flags or trade_count > 0))

    if ladder_ready:
        fidelity = "event_ladder"
        authority = "event_ladder"
        if bool(data.get("dom_fragmented_market")):
            authority = "fragmented_event_ladder"
        if stream_health_known and (stream_health_score < 0.50 or stream_degraded):
            authority = "degraded_event_ladder"
    elif depth_available and stream_snapshot_ready:
        fidelity = "stream_snapshot"
        authority = "snapshot_depth"
    elif depth_available and not synthetic_depth_available:
        fidelity = "snapshot_depth"
        authority = "snapshot_depth"
    elif synthetic_depth_available:
        fidelity = "synthetic_proxy"
        authority = "synthetic_proxy"
    else:
        fidelity = "none"
        authority = "none"

    provider_class = str(data.get("depth_provider_class") or "").strip().lower()
    if not provider_class:
        if any(token in source for token in ("binance", "bybit", "okx")):
            provider_class = "exchange"
        elif any(token in source for token in ("ctrader", "dukascopy", "ig", "deriv")):
            provider_class = "sidecar"
        elif synthetic_depth_available:
            provider_class = "synthetic"
        else:
            provider_class = "unknown"

    return {
        "depth_available": bool(depth_available),
        "synthetic_depth_available": bool(synthetic_depth_available),
        "depth_update_mode": update_mode,
        "depth_provider_class": provider_class,
        "dom_event_backed": bool(event_backed),
        "dom_ladder_ready": bool(ladder_ready),
        "dom_stream_snapshot_ready": bool(stream_snapshot_ready and depth_available),
        "dom_source_fidelity": fidelity,
        "dom_authority_tier": authority,
        "dom_evidence_strength": round(
            max(
                0.0,
                min(
                    1.0,
                    (0.25 if depth_available else 0.0)
                    + (0.28 if ladder_ready else 0.0)
                    + (0.16 if stream_snapshot_ready and depth_available else 0.0)
                    + min(depth_levels, 10) * 0.025
                    + min(delta_count + trade_count, 8) * 0.025
                    - (0.18 if stream_degraded else 0.0),
                ),
            ),
            4,
        ),
    }


def attach_dom_evidence(payload: Mapping[str, Any] | None = None, **overrides: Any) -> Dict[str, Any]:
    merged = dict(payload or {})
    merged.update(overrides)
    classified = classify_dom_evidence(merged)
    merged.update(classified)
    return merged
