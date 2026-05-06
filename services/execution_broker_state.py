from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from config.config import (
    BROKER_EXECUTION_PROVIDER,
    BROKER_EXECUTION_STATE_PATH,
    CTRADER_EXECUTION_BROKER_NAME,
    CTRADER_EXECUTION_ENABLED,
    EXECUTION_MODE,
    IG_EXECUTION_ENABLED,
)


_ALIASES = {
    "paper": "paper",
    "demo": "paper",
    "ig": "ig",
    "ig_demo": "ig",
    "ig_live": "ig",
    "ctrader": "ctrader",
    "ctrader_demo": "ctrader",
    "ctrader_live": "ctrader",
    "pepperstone": "ctrader",
    "pepperstone_ctrader": "ctrader",
}


def normalize_execution_broker(value: Any) -> str:
    key = str(value or "").strip().lower().replace("-", "_")
    return _ALIASES.get(key, key if key in {"paper", "ig", "ctrader"} else "")


def configured_execution_broker() -> str:
    provider = normalize_execution_broker(BROKER_EXECUTION_PROVIDER)
    if provider:
        return provider
    mode = normalize_execution_broker(EXECUTION_MODE)
    if mode in {"ig", "ctrader"}:
        return mode
    if CTRADER_EXECUTION_ENABLED:
        return "ctrader"
    if IG_EXECUTION_ENABLED:
        return "ig"
    return "paper"


def execution_broker_state_path() -> Path:
    path = Path(BROKER_EXECUTION_STATE_PATH)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def load_execution_broker_state() -> Dict[str, Any]:
    default_provider = configured_execution_broker()
    state: Dict[str, Any] = {
        "provider": default_provider,
        "broker_name": CTRADER_EXECUTION_BROKER_NAME if default_provider == "ctrader" else default_provider,
        "source": "config",
        "reason": "",
        "updated_at_utc": "",
    }
    path = execution_broker_state_path()
    try:
        if not path.exists():
            return state
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return state
    if not isinstance(payload, dict):
        return state
    provider = normalize_execution_broker(payload.get("provider"))
    if not provider:
        return state
    state.update(payload)
    state["provider"] = provider
    if not str(state.get("broker_name") or "").strip():
        state["broker_name"] = CTRADER_EXECUTION_BROKER_NAME if provider == "ctrader" else provider
    if provider == "ctrader":
        state["broker_name"] = "pepperstone"
    return state


def save_execution_broker_state(
    provider: Any,
    *,
    broker_name: str = "",
    source: str = "manual",
    reason: str = "",
) -> Dict[str, Any]:
    normalized = normalize_execution_broker(provider)
    if normalized not in {"paper", "ig", "ctrader"}:
        raise ValueError(f"unsupported execution broker: {provider or 'unknown'}")
    payload = {
        "provider": normalized,
        "broker_name": "pepperstone" if normalized == "ctrader" else str(broker_name or normalized).strip(),
        "source": str(source or "manual"),
        "reason": str(reason or "").strip(),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path = execution_broker_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    tmp.replace(path)
    return payload
