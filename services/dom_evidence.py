from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


_EVENT_STREAM_MODES = {"event_stream", "ladder_stream", "delta_stream"}
_DEPTH_DELTA_FLAGS = {"depth_delta", "book_delta", "ladder_delta"}
_DEPTH_SNAPSHOT_FLAGS = {"depth_snapshot"}
_STREAM_SNAPSHOT_FLAGS = {"stream_snapshot", "depth_stream"}
_TRADE_FLAGS = {"trade_print", "trade_stream", "tape_print"}


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return int(default)


def _flag_tokens(value: Any) -> set[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return set()
    normalized = raw.replace(";", ",").replace("|", ",")
    return {token.strip() for token in normalized.split(",") if token.strip()}


def classify_dom_evidence(payload: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    data = payload if isinstance(payload, Mapping) else {}
    depth_available = bool(data.get("depth_available"))
    synthetic_depth_available = bool(data.get("synthetic_depth_available"))
    flags = _flag_tokens(data.get("flags", data.get("dom_flags", "")))
    update_mode = str(data.get("depth_update_mode") or "").strip().lower()
    fragmentation_score = float(data.get("dom_fragmentation_score", 0.0) or 0.0)
    fragmented_market = bool(data.get("dom_fragmented_market"))
    stream_health_known = bool(data.get("dom_stream_health_known"))
    stream_health_score = float(data.get("dom_stream_health_score", 1.0) or 1.0)
    stream_degraded = bool(data.get("dom_stream_degraded"))
    depth_stream_missing = bool(data.get("dom_depth_stream_missing"))

    snapshot_count = max(
        _safe_int(data.get("dom_snapshot_count"), 0),
        1 if flags.intersection(_DEPTH_SNAPSHOT_FLAGS | _STREAM_SNAPSHOT_FLAGS) else 0,
    )
    delta_count = max(
        _safe_int(data.get("dom_delta_count"), 0),
        1 if flags.intersection(_DEPTH_DELTA_FLAGS) else 0,
    )
    trade_count = max(
        _safe_int(data.get("dom_trade_count"), 0),
        1 if flags.intersection(_TRADE_FLAGS) else 0,
    )

    dom_event_backed = bool(data.get("dom_event_backed"))
    if not dom_event_backed:
        dom_event_backed = bool(
            update_mode in _EVENT_STREAM_MODES
            or delta_count > 0
            or trade_count > 0
        )

    dom_stream_snapshot_ready = bool(data.get("dom_stream_snapshot_ready"))
    if not dom_stream_snapshot_ready:
        dom_stream_snapshot_ready = bool(
            depth_available
            and not dom_event_backed
            and (
                update_mode in {"stream_snapshot", "snapshot_stream"}
                or bool(flags.intersection(_STREAM_SNAPSHOT_FLAGS))
                or snapshot_count >= 3
            )
        )

    if not update_mode:
        if depth_available and dom_event_backed:
            update_mode = "event_stream"
        elif depth_available and dom_stream_snapshot_ready:
            update_mode = "stream_snapshot"
        elif depth_available and snapshot_count > 0:
            update_mode = "snapshot_poll"
        elif depth_available:
            update_mode = "top_of_book"
        elif synthetic_depth_available:
            update_mode = "synthetic"
        elif any(data.get(key) not in (None, "") for key in ("bid", "ask", "quote_price")):
            update_mode = "top_quote"
        else:
            update_mode = "none"

    dom_ladder_ready = bool(data.get("dom_ladder_ready"))
    if not dom_ladder_ready:
        dom_ladder_ready = bool(
            depth_available
            and dom_event_backed
            and (
                delta_count >= 2
                or (delta_count >= 1 and trade_count >= 1)
                or trade_count >= 3
            )
        )

    if synthetic_depth_available and not depth_available:
        source_fidelity = "synthetic"
        authority_tier = "synthetic_flow"
    elif depth_available and dom_ladder_ready:
        source_fidelity = "event_ladder"
        if stream_health_known and (stream_degraded or depth_stream_missing or stream_health_score < 0.58):
            authority_tier = "degraded_event_ladder"
        elif fragmented_market or fragmentation_score >= 0.42:
            authority_tier = "fragmented_event_ladder"
        else:
            authority_tier = "event_ladder"
    elif depth_available and dom_event_backed:
        source_fidelity = "event_partial"
        authority_tier = "snapshot_depth"
    elif depth_available and dom_stream_snapshot_ready:
        source_fidelity = "stream_snapshot"
        authority_tier = "snapshot_depth"
    elif depth_available and update_mode == "snapshot_poll":
        source_fidelity = "snapshot_depth"
        authority_tier = "snapshot_depth"
    elif depth_available:
        source_fidelity = "top_of_book"
        authority_tier = "snapshot_depth"
    elif update_mode == "top_quote":
        source_fidelity = "top_quote"
        authority_tier = "flow_only"
    else:
        source_fidelity = "none"
        authority_tier = "none"

    return {
        "depth_update_mode": update_mode,
        "dom_event_backed": bool(dom_event_backed),
        "dom_ladder_ready": bool(dom_ladder_ready),
        "dom_snapshot_count": int(max(0, snapshot_count)),
        "dom_delta_count": int(max(0, delta_count)),
        "dom_trade_count": int(max(0, trade_count)),
        "dom_stream_snapshot_ready": bool(dom_stream_snapshot_ready),
        "dom_source_fidelity": source_fidelity,
        "dom_authority_tier": authority_tier,
    }


def attach_dom_evidence(payload: Optional[Dict[str, Any]], **overrides: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(payload or {})
    for key, value in overrides.items():
        if value is not None:
            merged[key] = value
    merged.update(classify_dom_evidence(merged))
    return merged
