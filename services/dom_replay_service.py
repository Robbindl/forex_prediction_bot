from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, Optional


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return int(default)


class DomReplayService:
    def __init__(self, *, maxlen: int = 400, path: Optional[Path] = None) -> None:
        self._lock = threading.RLock()
        self._records: Deque[Dict[str, Any]] = deque(maxlen=max(50, int(maxlen)))
        self._path = Path(path) if path else Path(__file__).resolve().parent.parent / "runtime" / "dom_replay" / "dom_replay.jsonl"

    def _append(self, payload: Dict[str, Any]) -> None:
        record = dict(payload or {})
        with self._lock:
            self._records.append(record)
            try:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                with self._path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, default=str) + "\n")
            except Exception:
                pass

    @staticmethod
    def _decision_id(signal: Any) -> str:
        metadata = getattr(signal, "metadata", None)
        if not isinstance(metadata, dict):
            metadata = {}
            try:
                signal.metadata = metadata
            except Exception:
                return str(uuid.uuid4())
        decision_id = str(metadata.get("dom_replay_decision_id") or "").strip()
        if not decision_id:
            decision_id = uuid.uuid4().hex
            metadata["dom_replay_decision_id"] = decision_id
        return decision_id

    @staticmethod
    def _dom_snapshot(signal: Any) -> Dict[str, Any]:
        metadata = dict(getattr(signal, "metadata", {}) or {})
        return {
            "dom_source_fidelity": str(metadata.get("dom_source_fidelity") or ""),
            "dom_authority_tier": str(metadata.get("dom_authority_tier") or ""),
            "dom_stream_health_known": bool(metadata.get("dom_stream_health_known")),
            "dom_stream_connected": bool(metadata.get("dom_stream_connected")),
            "dom_stream_health_score": round(_safe_float(metadata.get("dom_stream_health_score")), 4),
            "dom_stream_trust_decay": round(_safe_float(metadata.get("dom_stream_trust_decay")), 4),
            "dom_stream_degraded": bool(metadata.get("dom_stream_degraded")),
            "dom_stream_reconnect_count": _safe_int(metadata.get("dom_stream_reconnect_count")),
            "dom_stream_sequence_gap_count": _safe_int(metadata.get("dom_stream_sequence_gap_count")),
            "dom_depth_stream_missing": bool(metadata.get("dom_depth_stream_missing")),
            "dom_trade_stream_missing": bool(metadata.get("dom_trade_stream_missing")),
            "dom_stream_health_hard_floor_breached": bool(
                metadata.get("dom_stream_health_hard_floor_breached")
            ),
            "dom_stream_health_blocks_sovereignty": bool(
                metadata.get("dom_stream_health_blocks_sovereignty")
            ),
            "depth_provider_trust_score": round(_safe_float(metadata.get("depth_provider_trust_score")), 4),
            "depth_provider_trust_score_effective": round(_safe_float(metadata.get("depth_provider_trust_score_effective")), 4),
            "depth_provider_trust_decay_applied": round(_safe_float(metadata.get("depth_provider_trust_decay_applied")), 4),
            "dom_fragmentation_score": round(_safe_float(metadata.get("dom_fragmentation_score")), 4),
            "dom_fragmented_market": bool(metadata.get("dom_fragmented_market")),
            "dom_trade_backed_iceberg_proxy": round(_safe_float(metadata.get("dom_trade_backed_iceberg_proxy")), 4),
            "dom_trade_absorption_proxy": round(_safe_float(metadata.get("dom_trade_absorption_proxy")), 4),
            "dom_refill_after_sweep_bias": round(_safe_float(metadata.get("dom_refill_after_sweep_bias")), 4),
            "dom_trade_aggression_bias": round(_safe_float(metadata.get("dom_trade_aggression_bias")), 4),
            "event_ladder_hostile_flow": bool(metadata.get("event_ladder_hostile_flow")),
            "event_ladder_hostile_flow_component_count": _safe_int(metadata.get("event_ladder_hostile_flow_component_count")),
            "event_ladder_cross_market_hard_block": bool(metadata.get("event_ladder_cross_market_hard_block")),
            "cross_asset_directional_conflict": bool(metadata.get("cross_asset_directional_conflict")),
        }

    def capture_signal_decision(self, signal: Any, context: Optional[Dict[str, Any]] = None) -> str:
        decision_id = self._decision_id(signal)
        metadata = dict(getattr(signal, "metadata", {}) or {})
        capture_signature = "|".join(
            [
                str(bool(getattr(signal, "alive", False))),
                str(getattr(signal, "step_reached", 0) or 0),
                str(round(_safe_float(getattr(signal, "confidence", 0.0)), 4)),
                str(getattr(signal, "kill_reason", "") or ""),
            ]
        )
        if metadata.get("_dom_replay_capture_signature") == capture_signature:
            return decision_id
        try:
            signal.metadata["_dom_replay_capture_signature"] = capture_signature
        except Exception:
            pass
        journal_summary = {}
        try:
            journal_summary = dict(signal.journal.summary(signal) or {})
        except Exception:
            journal_summary = {}
        metadata = dict(getattr(signal, "metadata", {}) or {})
        payload = {
            "record_type": "signal_decision",
            "ts": time.time(),
            "decision_id": decision_id,
            "asset": str(getattr(signal, "canonical_asset", "") or getattr(signal, "asset", "") or ""),
            "raw_asset": str(getattr(signal, "asset", "") or ""),
            "category": str(getattr(signal, "category", "") or ""),
            "direction": str(getattr(signal, "direction", "") or ""),
            "alive": bool(getattr(signal, "alive", False)),
            "kill_reason": str(getattr(signal, "kill_reason", "") or ""),
            "step_reached": _safe_int(getattr(signal, "step_reached", 0)),
            "final_confidence": round(_safe_float(getattr(signal, "confidence", 0.0)), 4),
            "playbook_name": str(metadata.get("playbook_name") or metadata.get("seed_model") or metadata.get("strategy_id") or getattr(signal, "strategy_id", "") or ""),
            "playbook_entry_style": str(metadata.get("playbook_entry_style") or ""),
            "blocked_reason": str(metadata.get("blocked_reason") or ""),
            "rejected_reasons": list(metadata.get("rejected_reasons") or []),
            "execution_hard_blocks": list(metadata.get("execution_hard_blocks") or []),
            "late_entry_risk_reasons": list(metadata.get("late_entry_risk_reasons") or []),
            "dom": self._dom_snapshot(signal),
            "factor_attribution": dict(journal_summary.get("factor_attribution") or {}),
            "setup_fingerprint": dict(journal_summary.get("setup_fingerprint") or {}),
            "journal_summary": journal_summary,
            "context": {
                "session": str(metadata.get("session_label") or metadata.get("playbook_session") or metadata.get("session") or ""),
                "timeframe": str(metadata.get("timeframe") or metadata.get("playbook_interval") or metadata.get("preferred_interval") or ""),
                "current_price": round(_safe_float((context or {}).get("current_price")), 6),
            },
        }
        self._append(payload)
        return decision_id

    def attach_trade_outcome(self, trade: Dict[str, Any]) -> None:
        trade_payload = dict(trade or {})
        metadata = dict(trade_payload.get("metadata") or {})
        decision_id = str(metadata.get("dom_replay_decision_id") or "").strip()
        if not decision_id:
            return
        payload = {
            "record_type": "trade_outcome",
            "ts": time.time(),
            "decision_id": decision_id,
            "trade_id": str(trade_payload.get("trade_id") or ""),
            "asset": str(trade_payload.get("canonical_asset") or trade_payload.get("asset") or ""),
            "direction": str(trade_payload.get("direction") or trade_payload.get("signal") or ""),
            "exit_reason": str(trade_payload.get("exit_reason") or ""),
            "pnl": round(_safe_float(trade_payload.get("pnl")), 4),
            "pnl_percent": round(_safe_float(trade_payload.get("pnl_percent")), 4),
            "risk_reward": round(_safe_float(trade_payload.get("risk_reward")), 4),
            "open_time": trade_payload.get("open_time"),
            "close_time": trade_payload.get("close_time"),
        }
        self._append(payload)

    def get_recent(self, limit: int = 50) -> list[Dict[str, Any]]:
        with self._lock:
            items = list(self._records)
        return items[-max(1, int(limit)) :]

    def clear(self) -> None:
        with self._lock:
            self._records.clear()


_service = DomReplayService()


def get_service() -> DomReplayService:
    return _service
