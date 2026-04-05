from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple

import pandas as pd

from core.signal import Signal
from strategies.base import BaseStrategy
from utils.logger import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger()

LIVE_STRATEGY_REGISTRY_PATH = Path("config/live_strategy_registry.json")
LIVE_STRATEGY_REGISTRY_VERSION = 1


class DynamicStrategyLive(BaseStrategy):
    """
    Wraps a strategy_lab DynamicStrategy config as a BaseStrategy.
    Compatible with the legacy strategy interface used by runtime injectors.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        min_confidence: float = 0.65,
    ) -> None:
        from strategy_lab.strategy_builder import StrategyBuilder

        self._config = copy.deepcopy(dict(config or {}))
        self._dynamic = StrategyBuilder.from_dict(self._config)
        self._min_conf = min_confidence
        self.name = str(self._config.get("name", "dynamic") or "dynamic")
        self.version = str(self._config.get("version", "1.0") or "1.0")

    def generate(
        self,
        asset: str,
        canonical: str,
        category: str,
        df: pd.DataFrame,
    ) -> Optional[Signal]:
        if df is None or len(df) < 50:
            return None
        try:
            result = self._dynamic.generate(df, asset=canonical, category=category)
            if result is None:
                return None
            if float(result.get("confidence", 0.0) or 0.0) < self._min_conf:
                return None
            return self._make_signal(
                asset=asset,
                canonical=canonical,
                category=category,
                direction=result["direction"],
                confidence=float(result["confidence"]),
                entry=float(result["entry_price"]),
                stop_loss=float(result["stop_loss"]),
                take_profit=float(result["take_profit"]),
                indicators=result.get("indicators", {}),
            )
        except Exception as e:
            logger.debug(f"[LiveBridge] {self.name} on {asset}: {e}")
            return None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clone_config(config: Any) -> Dict[str, Any]:
    return copy.deepcopy(dict(config or {}))


def _default_registry_payload() -> Dict[str, Any]:
    return {
        "version": LIVE_STRATEGY_REGISTRY_VERSION,
        "updated_at": "",
        "strategies": [],
    }


def _normalise_registry_entry(entry: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(entry, dict):
        return None
    config = entry.get("config")
    if not isinstance(config, dict):
        return None
    cloned_config = _clone_config(config)
    name = str(entry.get("name") or cloned_config.get("name") or "").strip()
    if not name:
        return None
    cloned_config.setdefault("name", name)
    return {
        "name": name,
        "enabled": bool(entry.get("enabled", True)),
        "config": cloned_config,
        "source": str(entry.get("source") or "registry"),
        "asset": str(entry.get("asset") or ""),
        "category": str(entry.get("category") or ""),
        "approved_at": str(entry.get("approved_at") or ""),
        "updated_at": str(entry.get("updated_at") or ""),
        "research_summary": copy.deepcopy(entry.get("research_summary") or {}),
    }


def _read_registry_payload(registry_path: Path | None = None) -> Dict[str, Any]:
    path = Path(registry_path or LIVE_STRATEGY_REGISTRY_PATH)
    if not path.exists():
        return _default_registry_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(f"[LiveBridge] Invalid live strategy registry at {path}: {exc}")
        return _default_registry_payload()
    if not isinstance(payload, dict):
        return _default_registry_payload()
    payload.setdefault("version", LIVE_STRATEGY_REGISTRY_VERSION)
    payload.setdefault("updated_at", "")
    if not isinstance(payload.get("strategies"), list):
        payload["strategies"] = []
    return payload


def _write_registry_payload(payload: Dict[str, Any], registry_path: Path | None = None) -> None:
    path = Path(registry_path or LIVE_STRATEGY_REGISTRY_PATH)
    safe_payload = copy.deepcopy(payload or {})
    safe_payload["version"] = LIVE_STRATEGY_REGISTRY_VERSION
    safe_payload["updated_at"] = _utc_now_iso()
    safe_payload.setdefault("strategies", [])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(safe_payload, indent=2, sort_keys=True), encoding="utf-8")


def load_registry_entries(registry_path: Path | None = None) -> List[Dict[str, Any]]:
    payload = _read_registry_payload(registry_path)
    rows: List[Dict[str, Any]] = []
    for raw in payload.get("strategies", []):
        entry = _normalise_registry_entry(raw)
        if entry is None or not entry.get("enabled", True):
            continue
        rows.append(entry)
    return rows


def find_live_registry_matches(
    asset: str,
    category: str,
    registry_path: Path | None = None,
) -> Dict[str, Any]:
    target_asset = str(asset or "").strip()
    target_category = str(category or "").strip()
    exact: List[Dict[str, Any]] = []
    category_matches: List[Dict[str, Any]] = []
    global_matches: List[Dict[str, Any]] = []

    for entry in load_registry_entries(registry_path):
        entry_asset = str(entry.get("asset") or "").strip()
        entry_category = str(entry.get("category") or "").strip()

        if entry_asset and target_asset and entry_asset == target_asset:
            exact.append(copy.deepcopy(entry))
            continue
        if not entry_asset and entry_category and target_category and entry_category == target_category:
            category_matches.append(copy.deepcopy(entry))
            continue
        if not entry_asset and not entry_category:
            global_matches.append(copy.deepcopy(entry))

    all_matches = exact + category_matches + global_matches
    if exact:
        match_scope = "asset"
    elif category_matches:
        match_scope = "category"
    elif global_matches:
        match_scope = "global"
    else:
        match_scope = "none"

    return {
        "asset": target_asset,
        "category": target_category,
        "matched": bool(all_matches),
        "exact_match": bool(exact),
        "match_scope": match_scope,
        "strategies": all_matches,
        "names": [str((entry.get("config") or {}).get("name") or entry.get("name") or "") for entry in all_matches],
    }


def has_live_strategy_approval(
    asset: str,
    category: str,
    registry_path: Path | None = None,
) -> bool:
    return bool(find_live_registry_matches(asset, category, registry_path).get("matched"))


def get_live_strategy_configs(registry_path: Path | None = None) -> List[Dict[str, Any]]:
    """
    Combine manual live configs with registry-managed configs.

    Manual configs win on name conflicts to preserve explicit source edits.
    """
    seen: set[str] = set()
    merged: List[Dict[str, Any]] = []
    for config in LIVE_STRATEGY_CONFIGS:
        cloned = _clone_config(config)
        name = str(cloned.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        merged.append(cloned)
    for entry in load_registry_entries(registry_path):
        cloned = _clone_config(entry.get("config") or {})
        name = str(cloned.get("name") or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        merged.append(cloned)
    return merged


def get_live_strategy_bundle(registry_path: Path | None = None) -> Tuple[List[Dict[str, Any]], str]:
    configs = get_live_strategy_configs(registry_path)
    signature = json.dumps(configs, sort_keys=True, separators=(",", ":"))
    return configs, signature


def promote_strategy_config(
    config: Dict[str, Any],
    *,
    report: Optional[Dict[str, Any]] = None,
    asset: str = "",
    category: str = "",
    source: str = "run_lab",
    registry_path: Path | None = None,
) -> Dict[str, Any]:
    """
    Persist one lab-approved strategy config into the live strategy registry.

    The live bot can read this registry without editing Python files.
    """
    cloned = _clone_config(config)
    name = str(cloned.get("name") or "").strip()
    if not name:
        raise ValueError("Strategy config is missing a name")
    cloned["name"] = name

    payload = _read_registry_payload(registry_path)
    retained = []
    for raw in payload.get("strategies", []):
        entry = _normalise_registry_entry(raw)
        if entry is None:
            continue
        if entry["name"] == name:
            continue
        retained.append(entry)

    research_summary = {}
    if isinstance(report, dict):
        research_summary = {
            "overall_score": float(report.get("overall_score", 0.0) or 0.0),
            "verdict": str(report.get("verdict", "unknown") or "unknown"),
            "research_profile": str(report.get("research_profile", "standard") or "standard"),
        }

    entry = {
        "name": name,
        "enabled": True,
        "config": cloned,
        "source": str(source or "run_lab"),
        "asset": str(asset or ""),
        "category": str(category or ""),
        "approved_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "research_summary": research_summary,
    }
    retained.append(entry)
    retained.sort(key=lambda item: str(item.get("name") or "").lower())
    payload["strategies"] = retained
    _write_registry_payload(payload, registry_path)
    logger.info(f"[LiveBridge] Promoted '{name}' into live registry")
    return copy.deepcopy(entry)


def sync_promoted_strategies(
    strategies: List[Dict[str, Any]],
    *,
    source: str = "bot_auto_research",
    registry_path: Path | None = None,
) -> List[Dict[str, Any]]:
    """
    Replace all registry entries from one source with the provided strategy set.

    This lets background automation keep its own managed live set current
    without touching manual source-controlled configs or entries from other
    promotion flows.
    """
    target_source = str(source or "bot_auto_research")
    payload = _read_registry_payload(registry_path)
    retained: List[Dict[str, Any]] = []
    incoming_names: set[str] = set()
    normalised_incoming: List[Dict[str, Any]] = []

    for item in list(strategies or []):
        if not isinstance(item, dict):
            continue
        config = _clone_config(item.get("config") or {})
        name = str(config.get("name") or item.get("name") or "").strip()
        if not name:
            continue
        config["name"] = name
        incoming_names.add(name)
        report = item.get("report") if isinstance(item.get("report"), dict) else {}
        normalised_incoming.append(
            {
                "name": name,
                "enabled": True,
                "config": config,
                "source": target_source,
                "asset": str(item.get("asset") or ""),
                "category": str(item.get("category") or ""),
                "approved_at": _utc_now_iso(),
                "updated_at": _utc_now_iso(),
                "research_summary": {
                    "overall_score": float(report.get("overall_score", 0.0) or 0.0),
                    "verdict": str(report.get("verdict", "unknown") or "unknown"),
                    "research_profile": str(report.get("research_profile", "standard") or "standard"),
                },
            }
        )

    for raw in payload.get("strategies", []):
        entry = _normalise_registry_entry(raw)
        if entry is None:
            continue
        if str(entry.get("source") or "") == target_source:
            continue
        if entry["name"] in incoming_names:
            continue
        retained.append(entry)

    retained.extend(normalised_incoming)
    retained.sort(key=lambda item: str(item.get("name") or "").lower())
    payload["strategies"] = retained
    _write_registry_payload(payload, registry_path)
    logger.info(
        f"[LiveBridge] Synced {len(normalised_incoming)} auto-managed live strategies "
        f"for source='{target_source}'"
    )
    return copy.deepcopy(normalised_incoming)


def remove_promoted_strategy(name: str, registry_path: Path | None = None) -> bool:
    target = str(name or "").strip()
    if not target:
        return False
    payload = _read_registry_payload(registry_path)
    before = len(payload.get("strategies", []))
    payload["strategies"] = [
        entry
        for entry in payload.get("strategies", [])
        if str((entry or {}).get("name") or "").strip() != target
    ]
    if len(payload["strategies"]) == before:
        return False
    _write_registry_payload(payload, registry_path)
    logger.info(f"[LiveBridge] Removed '{target}' from live registry")
    return True


def clear_promoted_strategies(registry_path: Path | None = None) -> int:
    payload = _read_registry_payload(registry_path)
    removed = len(payload.get("strategies", []))
    payload["strategies"] = []
    _write_registry_payload(payload, registry_path)
    return removed


# Edit this list only if you want hardcoded live strategies in source control.
# Registry-managed strategies are stored separately in LIVE_STRATEGY_REGISTRY_PATH.
LIVE_STRATEGY_CONFIGS: List[Dict[str, Any]] = [
    # StrategyBuilder.triple_ema_config(),
    # StrategyBuilder.golden_cross_config(),
    # StrategyBuilder.macd_rsi_confluence_config(),
    # StrategyBuilder.adx_ema_momentum_config(),
    # StrategyBuilder.bollinger_rsi_reversion_config(),
]


def list_live_strategies() -> List[str]:
    return [c.get("name", "unknown") for c in get_live_strategy_configs()]


__all__ = [
    "DynamicStrategyLive",
    "LIVE_STRATEGY_CONFIGS",
    "LIVE_STRATEGY_REGISTRY_PATH",
    "clear_promoted_strategies",
    "find_live_registry_matches",
    "get_live_strategy_bundle",
    "get_live_strategy_configs",
    "has_live_strategy_approval",
    "list_live_strategies",
    "load_registry_entries",
    "promote_strategy_config",
    "remove_promoted_strategy",
    "sync_promoted_strategies",
]
