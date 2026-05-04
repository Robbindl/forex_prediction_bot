"""
core/engine.py — TradingCore: single central engine.
"""
from __future__ import annotations

import json
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, cast

import pandas as pd

from config.config import (
    INACTIVITY_RELIEF_FULL_HOURS,
    INACTIVITY_RELIEF_START_HOURS,
    EXECUTION_MODE,
    IG_EXECUTION_ENABLED,
    MAX_SIGNAL_CONFIDENCE,
    MIN_FINAL_CONFIDENCE,
    PLAYBOOK_ONLY_RUNTIME,
    TOP_OPPORTUNITIES_LIMIT,
    TRADE_CLOSE_COOLDOWN_MINUTES as CONFIG_TRADE_CLOSE_COOLDOWN_MINUTES,
    WINNER_ADDON_ENABLED,
    WINNER_ADDON_MAX_POSITIONS_PER_ASSET,
    WINNER_ADDON_MIN_CONFIDENCE,
    WINNER_ADDON_MIN_PROGRESS_RR,
    WINNER_ADDON_MIN_QUALITY_SCORE,
    WINNER_ADDON_SIZE_FRACTION,
    get_timeframe_periods,
    get_trading_timeframe,
)
from utils.logger import get_logger
from core.signal import Signal
from core.decision_engine import SignalDecisionEngine, decision_engine as _global_decision_engine

TRADE_CLOSE_COOLDOWN_MINUTES = CONFIG_TRADE_CLOSE_COOLDOWN_MINUTES
TRADE_MIN_CONFIDENCE = MIN_FINAL_CONFIDENCE  # follow config value from .env

logger = get_logger()

COMMAND_CENTER_CONTEXT_LOG_KEY = "COMMAND_CENTER_CONTEXT_LOG"
COMMAND_CENTER_CONTEXT_UPDATE_CHANNEL = "COMMAND_CENTER_CONTEXT_UPDATE"
COMMAND_CENTER_CONTEXT_LOG_LIMIT = 199


def _get_news_event(category: str) -> dict:
    """Get current news event state — graceful fallback if monitor not started."""
    try:
        from data_ingestion.news_event_monitor import news_monitor
        return news_monitor.get_event_state(category)
    except Exception:
        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


def _seed_inactivity_profile(context: Dict[str, Any]) -> Dict[str, Any]:
    playbook_decision = context.get("playbook_decision")
    if isinstance(playbook_decision, dict):
        inactivity = playbook_decision.get("inactivity_profile")
        if isinstance(inactivity, dict):
            return {
                "active": bool(inactivity.get("active")),
                "hours_since_last_entry": float(inactivity.get("hours_since_last_entry", 0.0) or 0.0),
                "relief_strength": max(0.0, min(1.0, float(inactivity.get("relief_strength", 0.0) or 0.0))),
                "flat_book": bool(inactivity.get("flat_book")),
                "open_position_count": int(inactivity.get("open_position_count", 0) or 0),
                "equity_relief": bool(inactivity.get("equity_relief")),
                "equity_relief_strength": max(0.0, min(1.0, float(inactivity.get("equity_relief_strength", 0.0) or 0.0))),
                "category_recent_count": float(inactivity.get("category_recent_count", 0.0) or 0.0),
                "asset_recent_count": float(inactivity.get("asset_recent_count", 0.0) or 0.0),
            }

    adaptive_policy = context.get("adaptive_policy")
    if isinstance(adaptive_policy, dict):
        raw_policy = adaptive_policy.get("raw") if isinstance(adaptive_policy.get("raw"), dict) else adaptive_policy
        inactivity = raw_policy.get("inactivity_profile") if isinstance(raw_policy, dict) else None
        if isinstance(inactivity, dict):
            return {
                "active": bool(inactivity.get("active")),
                "hours_since_last_entry": float(inactivity.get("hours_since_last_entry", 0.0) or 0.0),
                "relief_strength": max(0.0, min(1.0, float(inactivity.get("relief_strength", 0.0) or 0.0))),
                "flat_book": bool(inactivity.get("flat_book")),
                "open_position_count": int(inactivity.get("open_position_count", 0) or 0),
                "equity_relief": bool(inactivity.get("equity_relief")),
                "equity_relief_strength": max(0.0, min(1.0, float(inactivity.get("equity_relief_strength", 0.0) or 0.0))),
                "category_recent_count": float(inactivity.get("category_recent_count", 0.0) or 0.0),
                "asset_recent_count": float(inactivity.get("asset_recent_count", 0.0) or 0.0),
            }

    engine = context.get("engine")
    state = getattr(engine, "state", None) if engine is not None else context.get("state")
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
    relief_strength = max(0.0, min(1.0, (float(hours_since_last_entry) - start_hours) / max(1.0, full_hours - start_hours)))
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


class TradingCore:
    """Central trading engine — single instance per process."""

    def __init__(
        self,
        balance: float = 10000.0,
        strategy_mode: str = "policy",
        no_telegram: bool = False,
    ) -> None:
        self.balance       = balance
        self.strategy_mode = strategy_mode
        self.no_telegram   = no_telegram

        from core.state  import state as shared_state
        from core.events import EventBus
        from core.assets import AssetRegistry

        self.state    = shared_state
        self.events   = EventBus()
        self.registry = AssetRegistry()
        self.decision_engine: SignalDecisionEngine = _global_decision_engine

        try:
            self.state.init_db()
        except Exception as e:
            logger.debug(f"[TradingCore] Shared state DB sync skipped: {e}")

        from pathlib import Path as _Path
        _state_file_exists = _Path("data/system_state.json").exists()

        if self.state.open_position_count() == 0 and not _state_file_exists:
            # First ever run — no saved state at all — use the supplied balance
            self.state.set_balance(balance, "startup")
            logger.info(f"[TradingCore] Fresh start — balance=${balance}")
        else:
            # Restart — preserve accumulated balance from previous session
            logger.info(
                f"[TradingCore] Restored balance=${self.state.balance:.2f} "
                f"positions={self.state.open_position_count()}"
            )

        self.telegram:    Optional[Any] = None
        self.fetcher:     Optional[Any] = None
        self._strategy:   Optional[Any] = None   # reserved for compatibility wiring
        self._predictor:  Optional[Any] = None   # reserved for external prediction client
        self._agent:      Optional[Any] = None   # TradingAgent singleton

        self._engine_ready = threading.Event()
        self._stop_event   = threading.Event()
        self._is_running   = False
        self._loop_thread: Optional[threading.Thread] = None
        self._paper_trader: Optional[Any] = None
        self._risk_manager: Optional[Any] = None
        self._portfolio_risk: Optional[Any] = None
        self._last_ranked_opportunities: List[Dict[str, Any]] = []
        self._last_ranked_at_utc: str = ""
        self._last_broker_balance_sync = 0.0
        self._last_broker_balance_snapshot: Dict[str, Any] = {}
        self._broker_issue_alerts: Dict[str, float] = {}

        logger.info(f"[TradingCore] Init — balance=${balance} strategy={strategy_mode}")

    # ── Startup / Shutdown ────────────────────────────────────────────────────

    def start(self) -> None:
        if self._is_running:
            logger.warning("[TradingCore] Already running")
            return
        self._is_running = True
        self._stop_event.clear()
        self._loop_thread = threading.Thread(
            target=self._run, name="TradingCore-loop", daemon=True
        )
        self._loop_thread.start()
        logger.info("[TradingCore] Trading loop started")

    def stop(self, reason: str = "manual") -> None:
        if not self._is_running:
            return
        logger.info(f"[TradingCore] Stopping — {reason}")
        self._stop_event.set()
        self._is_running = False
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)
        self.state.force_save()
        logger.info("[TradingCore] Stopped")

    def wait_until_ready(self, timeout: float = 60.0) -> bool:
        return self._engine_ready.wait(timeout=timeout)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_ready(self) -> bool:
        return self._engine_ready.is_set()

    # ── Public API ────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Dict]:
        return self.state.get_open_positions()

    def get_closed_trades(self, limit: int = 100) -> List[Dict]:
        return self.state.get_closed_positions(limit=limit)

    def get_balance(self) -> float:
        self._sync_broker_account_balance()
        return self.state.balance

    def get_performance(self) -> Dict:
        return self.state.get_performance()

    def get_daily_stats(self) -> Dict:
        return {"daily_trades": self.state.daily_trades, "daily_pnl": self.state.daily_pnl}

    def get_runtime_asset_snapshot(self, asset: str) -> Dict[str, Any]:
        canonical = self.registry.canonical(asset)
        category = self.registry.category(canonical)
        market_open, market_reason = self._market_hours_status(canonical, category)
        snapshot: Dict[str, Any] = {
            "asset": canonical,
            "canonical_asset": canonical,
            "category": category,
            "market_status": {"market_open": bool(market_open), "reason": str(market_reason or "")},
            "decision_status": "snapshot_only",
            "decision_reason": "snapshot_only",
            "signal": {},
            "market_structure": {},
            "market_intelligence": {},
            "playbook_decision": {},
            "sentiment_score": 0.0,
            "current_price": 0.0,
            "latest_close": 0.0,
            "open_position": None,
        }

        open_position = next(
            (
                dict(pos)
                for pos in self.state.get_open_positions()
                if self.registry.canonical(str(pos.get("asset", "") or "")) == canonical
            ),
            None,
        )
        snapshot["open_position"] = open_position
        if open_position:
            metadata = dict(open_position.get("metadata") or {})
            direction = str(open_position.get("direction") or open_position.get("signal") or "HOLD").upper()
            snapshot.update(
                {
                    "decision_status": "open_position",
                    "decision_reason": "open_position_active",
                    "sentiment_score": float(metadata.get("sentiment_score", 0.0) or 0.0),
                    "current_price": float(open_position.get("current_price", 0.0) or 0.0),
                    "signal": {
                        "direction": direction,
                        "confidence": float(open_position.get("confidence", 0.0) or metadata.get("confidence", 0.0) or 0.0),
                        "alive": direction != "HOLD",
                        "entry_price": open_position.get("entry_price"),
                        "stop_loss": open_position.get("stop_loss"),
                        "take_profit": open_position.get("take_profit"),
                        "metadata": metadata,
                    },
                }
            )
            return snapshot

        try:
            ranked = self.get_top_ranked_opportunities(limit=max(3, TOP_OPPORTUNITIES_LIMIT), refresh=False, allow_refresh_when_empty=False)
        except TypeError:
            ranked = self.get_top_ranked_opportunities(limit=max(3, TOP_OPPORTUNITIES_LIMIT), refresh=False)
        except Exception:
            ranked = []

        candidate = next(
            (
                dict(item)
                for item in ranked
                if self.registry.canonical(str(item.get("asset", "") or "")) == canonical
            ),
            None,
        )
        if candidate:
            direction = str(candidate.get("direction") or candidate.get("signal") or "HOLD").upper()
            snapshot.update(
                {
                    "decision_status": "cached_ranked_setup",
                    "decision_reason": "cached_ranked_setup",
                    "sentiment_score": float(candidate.get("sentiment_score", 0.0) or 0.0),
                    "current_price": float(candidate.get("current_price", 0.0) or 0.0),
                    "signal": {
                        "direction": direction,
                        "confidence": float(candidate.get("confidence", 0.0) or 0.0),
                        "alive": direction != "HOLD",
                        "entry_price": candidate.get("entry_price"),
                        "stop_loss": candidate.get("stop_loss"),
                        "take_profit": candidate.get("take_profit"),
                        "metadata": {},
                    },
                    "playbook_decision": dict(candidate.get("opportunity_breakdown") or {}),
                }
            )
        return snapshot

    @staticmethod
    def _market_hours_status_fallback(asset: str, category: str) -> Tuple[bool, str]:
        try:
            from services.market_hours_guard import build_market_status

            status = build_market_status(asset, category)
            if status and "market_open" in status:
                return bool(status["market_open"]), str(status.get("reason", "market status"))
        except Exception:
            pass

        now_utc = datetime.now(tz=timezone.utc)
        wd = now_utc.weekday()
        hour = now_utc.hour

        if category == "crypto":
            return True, "crypto_24x7"

        if wd >= 5:
            if wd == 6 and hour >= 22 and category in ("forex", "commodities"):
                return True, "sunday_reopen"
            return False, "weekend_closed"

        if category == "forex" and wd == 4 and hour >= 22:
            return False, "forex_friday_close"

        if category in ("stocks", "indices") and not (13 <= hour < 21):
            return False, "indices_out_of_session"

        if category == "commodities" and hour == 21:
            return False, "commodities_settlement"

        return True, "open"

    def _extract_position_action_metrics(
        self,
        pos: Dict[str, Any],
        *,
        include_market_status: bool = True,
    ) -> Dict[str, Any]:
        meta = dict(pos.get("metadata") or {})
        memory = meta.get("setup_memory") if isinstance(meta.get("setup_memory"), dict) else {}
        execution = meta.get("execution_feedback") if isinstance(meta.get("execution_feedback"), dict) else {}
        execution_policy = (
            meta.get("execution_feedback_policy")
            if isinstance(meta.get("execution_feedback_policy"), dict)
            else {}
        )

        asset = str(pos.get("asset", "") or "")
        category = str(pos.get("category", "") or "forex")
        confidence = float(pos.get("confidence", 0.0) or 0.0)
        opportunity_score = float(meta.get("opportunity_score", 0.0) or 0.0)
        memory_score = float(meta.get("memory_score", memory.get("memory_score", 0.0)) or 0.0)
        memory_sample_count = int(meta.get("memory_sample_count", memory.get("sample_count", 0)) or 0)
        execution_quality_score = float(
            execution.get(
                "quality_score",
                meta.get("execution_quality_score", execution_policy.get("avg_quality_score", 0.0)),
            )
            or 0.0
        )
        execution_sample_count = int(
            meta.get(
                "execution_feedback_sample_count",
                execution_policy.get("sample_count", execution.get("sample_count", 0)),
            )
            or 0
        )
        pnl = float(pos.get("pnl", 0.0) or 0.0)
        risk_reward = float(pos.get("risk_reward", 0.0) or 0.0)

        memory_component = max(0.0, min(1.0, memory_score / 100.0)) if memory_sample_count > 0 else 0.5
        execution_component = (
            max(0.0, min(1.0, execution_quality_score / 100.0))
            if execution_sample_count > 0
            else 0.5
        )
        opportunity_component = (
            max(0.0, min(1.0, opportunity_score))
            if opportunity_score > 0
            else max(0.0, min(1.0, confidence))
        )
        confidence_component = max(0.0, min(1.0, confidence)) if confidence > 0 else 0.5
        rr_component = max(0.0, min(1.0, (risk_reward - 1.0) / 1.5)) if risk_reward > 0 else 0.5
        pnl_component = 0.40 if pnl < 0 else (0.58 if abs(pnl) < 1e-9 else 0.72)
        if include_market_status:
            market_open, market_reason = self._market_hours_status(asset, category)
        else:
            market_open, market_reason = self._market_hours_status_fallback(asset, category)

        quality_ratio = max(
            0.0,
            min(
                1.0,
                (
                    opportunity_component * 0.32
                    + execution_component * 0.24
                    + memory_component * 0.18
                    + confidence_component * 0.16
                    + rr_component * 0.05
                    + pnl_component * 0.05
                ),
            ),
        )

        weak_reasons: List[str] = []
        if opportunity_component < 0.58:
            weak_reasons.append("opportunity weak")
        if memory_sample_count >= 5 and memory_component < 0.55:
            weak_reasons.append("memory weak")
        if execution_sample_count >= 5 and execution_component < 0.55:
            weak_reasons.append("execution weak")
        if confidence_component < max(0.45, TRADE_MIN_CONFIDENCE - 0.03):
            weak_reasons.append("confidence fading")
        if pnl < 0:
            weak_reasons.append("losing live")
        if risk_reward > 0 and rr_component < 0.40:
            weak_reasons.append("compressed rr")
        if not market_open:
            weak_reasons.append(f"market closed ({market_reason})")

        return {
            "trade_id": str(pos.get("trade_id", "") or ""),
            "asset": asset,
            "category": category,
            "direction": str(pos.get("direction") or pos.get("signal") or "BUY").upper(),
            "quality_ratio": round(quality_ratio, 4),
            "quality_score": round(quality_ratio * 100.0, 1),
            "memory_score": round(memory_score, 1),
            "memory_sample_count": memory_sample_count,
            "execution_quality_score": round(execution_quality_score, 1),
            "execution_feedback_sample_count": execution_sample_count,
            "opportunity_score": round(opportunity_score, 4),
            "confidence": round(confidence, 4),
            "pnl": round(pnl, 4),
            "risk_reward": round(risk_reward, 4),
            "market_open": market_open,
            "market_reason": market_reason,
            "weak_reasons": weak_reasons,
            "is_weak": quality_ratio < 0.58 or len(weak_reasons) >= 2,
        }

    def _open_positions_for_asset(self, canonical_asset: str) -> List[Dict[str, Any]]:
        return [
            dict(pos)
            for pos in self.state.get_open_positions()
            if self.registry.canonical(
                str(pos.get("canonical_asset") or pos.get("asset") or "")
            ) == canonical_asset
        ]

    def _winner_addon_scan_allowed(self, canonical_asset: str) -> bool:
        if not WINNER_ADDON_ENABLED:
            return False
        positions = self._open_positions_for_asset(canonical_asset)
        if not positions:
            return False
        if len(positions) >= max(1, int(WINNER_ADDON_MAX_POSITIONS_PER_ASSET or 2)):
            return False
        return True

    @staticmethod
    def _position_progress_rr(pos: Dict[str, Any], current_price: float) -> float:
        entry = float(pos.get("entry_price", 0.0) or 0.0)
        stop_loss = float(pos.get("original_sl", pos.get("stop_loss", 0.0)) or 0.0)
        if entry <= 0 or current_price <= 0:
            return 0.0
        risk = abs(entry - stop_loss)
        if risk <= 0:
            return 0.0
        direction = str(pos.get("direction") or pos.get("signal") or "BUY").upper()
        if direction == "BUY":
            return (current_price - entry) / max(risk, 1e-9)
        return (entry - current_price) / max(risk, 1e-9)

    def _build_winner_addon_plan(
        self,
        canonical_asset: str,
        category: str,
        signal: Signal,
        current_price: float,
    ) -> Dict[str, Any]:
        if not WINNER_ADDON_ENABLED:
            return {}
        positions = self._open_positions_for_asset(canonical_asset)
        if not positions:
            return {}
        if len(positions) >= max(1, int(WINNER_ADDON_MAX_POSITIONS_PER_ASSET or 2)):
            return {}

        primary = next(
            (
                pos
                for pos in positions
                if not bool(((pos.get("metadata") or {}).get("winner_addon") or {}).get("enabled"))
            ),
            positions[0],
        )
        primary_direction = str(primary.get("direction") or primary.get("signal") or "BUY").upper()
        if primary_direction != str(signal.direction or "BUY").upper():
            return {}
        if float(signal.confidence or 0.0) < float(WINNER_ADDON_MIN_CONFIDENCE or 0.66):
            return {}

        quality = self._extract_position_action_metrics(primary, include_market_status=False)
        quality_score = float(quality.get("quality_score", 0.0) or 0.0)
        progress_rr = self._position_progress_rr(primary, float(current_price or 0.0))
        if progress_rr < float(WINNER_ADDON_MIN_PROGRESS_RR or 1.5):
            return {}
        if quality_score < float(WINNER_ADDON_MIN_QUALITY_SCORE or 62.0):
            return {}
        if float(primary.get("pnl", 0.0) or 0.0) <= 0.0:
            return {}

        return {
            "enabled": True,
            "asset": canonical_asset,
            "category": category,
            "parent_trade_id": str(primary.get("trade_id", "") or ""),
            "parent_direction": primary_direction,
            "progress_rr": round(progress_rr, 4),
            "parent_quality_score": round(quality_score, 1),
            "size_fraction": round(max(0.15, min(0.70, float(WINNER_ADDON_SIZE_FRACTION or 0.45))), 4),
        }

    def get_weak_positions(
        self,
        limit: int = 5,
        score_threshold: float = 0.58,
        *,
        include_market_status: bool = True,
    ) -> List[Dict[str, Any]]:
        candidates = []
        for pos in self.state.get_open_positions():
            metrics = self._extract_position_action_metrics(
                pos,
                include_market_status=include_market_status,
            )
            if metrics["quality_ratio"] <= float(score_threshold or 0.58) or len(metrics["weak_reasons"]) >= 2:
                candidates.append(metrics)
        candidates.sort(
            key=lambda item: (
                float(item.get("quality_ratio", 1.0) or 1.0),
                -len(item.get("weak_reasons", [])),
                float(item.get("pnl", 0.0) or 0.0),
            )
        )
        return candidates[: max(1, int(limit or 5))]

    def _build_reprice_snapshot(
        self,
        pos: Dict[str, Any],
        *,
        tighten_only: bool,
    ) -> Optional[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
        trade_id = str(pos.get("trade_id", "") or "")
        asset = str(pos.get("asset", "") or "")
        category = str(pos.get("category", "") or "")
        direction = str(pos.get("direction") or pos.get("signal") or "BUY").upper()
        entry = float(pos.get("entry_price", 0) or 0)
        current_sl = float(pos.get("stop_loss", 0) or 0)
        current_tp = float(pos.get("take_profit", 0) or 0)
        current_original_sl = float(pos.get("original_sl", current_sl) or current_sl)

        if not trade_id or not asset or entry <= 0 or direction not in {"BUY", "SELL"}:
            return None

        price_data = self._fetch_price_data(asset, category)
        atr = self._estimate_atr(price_data)
        execution_feedback_policy = self._load_execution_feedback_policy(
            asset=asset,
            category=category,
            context={"position": pos},
        )
        stop_buffer_multiplier, target_rr_multiplier = self._execution_feedback_multipliers(
            execution_feedback_policy
        )
        pos_meta = pos.get("metadata")
        structure = pos_meta.get("market_structure") if isinstance(pos_meta, dict) else {}

        proposed_sl = self._resolve_stop_loss(
            entry_price=entry,
            direction=direction,
            category=category,
            atr=atr,
            stop_buffer_multiplier=stop_buffer_multiplier,
            structure=structure if isinstance(structure, dict) else {},
        )
        effective_sl = self._tighten_stop_loss(
            current_stop_loss=current_sl,
            proposed_stop_loss=proposed_sl,
            direction=direction,
            tighten_only=tighten_only,
        )
        effective_tp = self._resolve_take_profit(
            entry_price=entry,
            stop_loss=effective_sl,
            direction=direction,
            category=category,
            atr=atr,
            target_rr_multiplier=target_rr_multiplier,
        )
        effective_tp, structure_target_alignment = self._align_take_profit_to_structure(
            asset=asset,
            entry_price=entry,
            take_profit=effective_tp,
            direction=direction,
            category=category,
            structure=structure if isinstance(structure, dict) else {},
            atr=atr,
            confidence=float(pos.get("confidence", 0.0) or 0.0),
        )

        snapshot = dict(pos)
        snapshot["stop_loss"] = float(round(effective_sl, 6))
        snapshot["take_profit"] = float(round(effective_tp, 6))
        snapshot["take_profit_levels"] = self._build_take_profit_levels(
            entry,
            effective_tp,
            direction,
            structure=structure if isinstance(structure, dict) else {},
        )
        snapshot["risk_reward"] = round(
            abs(snapshot["take_profit"] - entry) / max(abs(entry - snapshot["stop_loss"]), 1e-9),
            4,
        )
        if not current_original_sl or abs(current_original_sl - current_sl) < 1e-9:
            snapshot["original_sl"] = float(round(effective_sl, 6))
        snapshot["metadata"] = self._build_reprice_metadata(
            snapshot=snapshot,
            atr=atr,
            execution_feedback_policy=execution_feedback_policy,
            target_rr_multiplier=target_rr_multiplier,
            stop_buffer_multiplier=stop_buffer_multiplier,
            structure_target_alignment=structure_target_alignment,
        )

        sl_changed = abs(snapshot["stop_loss"] - current_sl) > 1e-9
        tp_changed = abs(snapshot["take_profit"] - current_tp) > 1e-9
        if not (sl_changed or tp_changed):
            return None

        update = self._build_reprice_update(
            trade_id=trade_id,
            asset=asset,
            category=category,
            direction=direction,
            entry_price=entry,
            current_stop_loss=current_sl,
            current_take_profit=current_tp,
            snapshot=snapshot,
            atr=atr,
            execution_feedback_policy=execution_feedback_policy,
            target_rr_multiplier=target_rr_multiplier,
            stop_buffer_multiplier=stop_buffer_multiplier,
        )
        return trade_id, snapshot, update

    def _sync_repriced_snapshot(self, trade_id: str, snapshot: Dict[str, Any]) -> None:
        self.state.sync_open_position(snapshot)
        if self._paper_trader is not None:
            with self._paper_trader._lock:
                if trade_id in self._paper_trader.open_positions:
                    self._paper_trader.open_positions[trade_id].update(snapshot)

    def reprice_open_positions(
        self,
        tighten_only: bool = True,
        trade_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Recalculate SL/TP for open positions using the current ATR-based framework."""
        if self.fetcher is None or self._risk_manager is None:
            self._init_subsystems()

        trade_id_filter = {str(tid) for tid in trade_ids or [] if str(tid or "").strip()}
        updates: List[Dict[str, Any]] = []
        for pos in self.state.get_open_positions():
            trade_id = str(pos.get("trade_id", "") or "")
            if trade_id_filter and trade_id not in trade_id_filter:
                continue
            priced = self._build_reprice_snapshot(pos, tighten_only=tighten_only)
            if priced is None:
                continue
            trade_id, snapshot, update = priced
            self._sync_repriced_snapshot(trade_id, snapshot)
            updates.append(update)

        return updates

    def reprice_weak_exits(
        self,
        tighten_only: bool = True,
        limit: int = 3,
        score_threshold: float = 0.62,
    ) -> List[Dict[str, Any]]:
        candidates = self.get_weak_positions(limit=limit, score_threshold=score_threshold)
        if not candidates:
            return []

        updates = self.reprice_open_positions(
            tighten_only=tighten_only,
            trade_ids=[item["trade_id"] for item in candidates],
        )
        by_trade_id = {item["trade_id"]: item for item in candidates}
        for update in updates:
            metrics = by_trade_id.get(str(update.get("trade_id", "") or ""), {})
            update["quality_score"] = float(metrics.get("quality_score", 0.0) or 0.0)
            update["weak_reasons"] = list(metrics.get("weak_reasons", []))
        return updates

    def _record_partial_reduction(
        self,
        parent_snapshot: Dict[str, Any],
        partial_trade: Dict[str, Any],
        pnl: float,
    ) -> None:
        if self._paper_trader and callable(getattr(self._paper_trader, "on_trade_closed", None)):
            self._paper_trader.on_trade_closed(partial_trade)
            return

        recorded = self.state.record_partial_close(str(parent_snapshot.get("trade_id", "") or ""), partial_trade)
        if not recorded:
            return
        self._record_trade_close_side_effects(recorded, pnl)

    @staticmethod
    def _normalized_reduction_fraction(reduction_fraction: float) -> float:
        return max(0.10, min(0.75, float(reduction_fraction or 0.35)))

    def _get_weak_position_exit_snapshot(self, pos: Dict[str, Any]) -> Tuple[float, float]:
        asset = str(pos.get("asset", "") or "")
        category = str(pos.get("category", "") or "forex")
        entry_price = float(pos.get("entry_price", 0.0) or 0.0)
        position_size = float(pos.get("position_size", 0.0) or 0.0)
        direction = str(pos.get("direction") or pos.get("signal") or "BUY").upper()

        exit_price = entry_price
        try:
            if self.fetcher is not None:
                live_price, _spread = self.fetcher.get_real_time_price(asset, category)
                if live_price:
                    exit_price = float(live_price)
        except Exception:
            pass

        try:
            from risk.position_sizer import PositionSizer as _PS

            total_live_pnl = _PS.pnl(asset, category, entry_price, exit_price, position_size, direction)
        except Exception:
            total_live_pnl = (
                (exit_price - entry_price) * position_size
                if direction == "BUY"
                else (entry_price - exit_price) * position_size
            )
        return float(exit_price), float(total_live_pnl)

    @staticmethod
    def _build_reduction_note(
        item: Dict[str, Any],
        reduction_fraction: float,
    ) -> Dict[str, Any]:
        return {
            "reduced_at_utc": datetime.utcnow().isoformat(),
            "reduction_fraction": round(reduction_fraction, 4),
            "quality_score": float(item.get("quality_score", 0.0) or 0.0),
            "weak_reasons": list(item.get("weak_reasons", [])),
        }

    def _sync_reduced_parent_position(self, trade_id: str, parent_snapshot: Dict[str, Any]) -> None:
        self.state.sync_open_position(parent_snapshot)
        if self._paper_trader is not None:
            with self._paper_trader._lock:
                if trade_id in self._paper_trader.open_positions:
                    self._paper_trader.open_positions[trade_id].update(parent_snapshot)
            notify_update = getattr(self._paper_trader, "_notify_position_updated", None)
            if callable(notify_update):
                notify_update(parent_snapshot)

    @staticmethod
    def _build_weak_reduction_partial_trade(
        pos: Dict[str, Any],
        *,
        trade_id: str,
        reduction_size: float,
        exit_price: float,
        partial_pnl: float,
        updated_metadata: Dict[str, Any],
        reduction_note: Dict[str, Any],
        reduction_fraction: float,
    ) -> Dict[str, Any]:
        from execution.paper_trader import PaperTrader

        return PaperTrader._close(
            dict(
                pos,
                trade_id=f"{trade_id}-RW{int(time.time() * 1000) % 1000000}",
                parent_trade_id=trade_id,
                is_partial_close=True,
                position_size=reduction_size,
                metadata={
                    **updated_metadata,
                    "reduction_action": reduction_note,
                },
            ),
            exit_price,
            f"Weak Position Reduction {int(round(reduction_fraction * 100))}%",
            partial_pnl,
        )

    def reduce_weak_positions(
        self,
        reduction_fraction: float = 0.35,
        limit: int = 3,
        score_threshold: float = 0.58,
    ) -> List[Dict[str, Any]]:
        if self.fetcher is None or self._paper_trader is None:
            self._init_subsystems()

        fraction = self._normalized_reduction_fraction(reduction_fraction)
        candidates = self.get_weak_positions(limit=limit, score_threshold=score_threshold)
        if not candidates:
            return []

        actions: List[Dict[str, Any]] = []
        for item in candidates:
            trade_id = str(item.get("trade_id", "") or "")
            pos = self.state.get_open_position(trade_id)
            if not pos:
                continue

            asset = str(pos.get("asset", "") or "")
            category = str(pos.get("category", "") or "forex")
            market_open, market_reason = self._market_hours_status(asset, category)
            if not market_open:
                actions.append(
                    {
                        **item,
                        "success": False,
                        "action": "skipped",
                        "reason": market_reason,
                    }
                )
                continue

            entry_price = float(pos.get("entry_price", 0.0) or 0.0)
            position_size = float(pos.get("position_size", 0.0) or 0.0)
            direction = str(pos.get("direction") or pos.get("signal") or "BUY").upper()
            if entry_price <= 0.0 or position_size <= 0.0:
                continue

            exit_price, total_live_pnl = self._get_weak_position_exit_snapshot(pos)

            reduction_size = round(position_size * fraction, 8)
            remaining_size = round(position_size - reduction_size, 8)
            if reduction_size <= 0.0 or remaining_size <= 0.0:
                continue

            partial_pnl = float(total_live_pnl) * fraction
            remaining_pnl = float(total_live_pnl) - partial_pnl

            metadata = dict(pos.get("metadata") or {})
            reduction_note = self._build_reduction_note(item, fraction)
            updated_metadata = {
                **metadata,
                "manual_reduction": reduction_note,
                "execution_notes": list(dict(metadata).get("execution_notes", []) or []),
            }

            parent_snapshot = dict(pos)
            parent_snapshot["position_size"] = remaining_size
            parent_snapshot["pnl"] = round(remaining_pnl, 6)
            parent_snapshot["metadata"] = updated_metadata

            self._sync_reduced_parent_position(trade_id, parent_snapshot)
            partial_trade = self._build_weak_reduction_partial_trade(
                pos,
                trade_id=trade_id,
                reduction_size=reduction_size,
                exit_price=exit_price,
                partial_pnl=partial_pnl,
                updated_metadata=updated_metadata,
                reduction_note=reduction_note,
                reduction_fraction=fraction,
            )

            self._record_partial_reduction(parent_snapshot, partial_trade, partial_pnl)
            logger.info(
                f"[TradingCore] Reduced weak position {asset} by {fraction:.0%} "
                f"(quality={float(item.get('quality_score', 0.0) or 0.0):.1f})"
            )
            actions.append(
                {
                    **item,
                    "success": True,
                    "action": "reduced",
                    "reduction_fraction": round(fraction, 4),
                    "reduced_size": reduction_size,
                    "remaining_size": remaining_size,
                    "exit_price": round(exit_price, 6),
                    "realized_pnl": round(partial_pnl, 6),
                }
            )

        return actions

    def _remember_ranked_opportunities(
        self,
        ranked_pairs: List[Tuple[Signal, Dict[str, Any]]],
    ) -> None:
        self._last_ranked_opportunities = []
        self._last_ranked_at_utc = datetime.utcnow().isoformat()
        for signal, context in ranked_pairs[:10]:
            self._last_ranked_opportunities.append(
                self._build_ranked_signal_snapshot(signal, context)
            )

    def _build_ranked_signal_snapshot(
        self,
        signal: Signal,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        item = signal.to_dict()
        metadata = dict(signal.metadata or {})
        broker = metadata.get("broker_quality") if isinstance(metadata.get("broker_quality"), dict) else {}
        micro = metadata.get("market_microstructure") if isinstance(metadata.get("market_microstructure"), dict) else {}
        cross_asset = metadata.get("cross_asset_context") if isinstance(metadata.get("cross_asset_context"), dict) else {}
        adaptive_policy = metadata.get("adaptive_policy") if isinstance(metadata.get("adaptive_policy"), dict) else {}
        recent_review = (
            adaptive_policy.get("recent_review_profile")
            if isinstance(adaptive_policy.get("recent_review_profile"), dict)
            else {}
        )
        breakdown = metadata.get("opportunity_breakdown") if isinstance(metadata.get("opportunity_breakdown"), dict) else {}
        broker_quality_score = broker.get(
            "score",
            metadata.get("broker_quality_score", breakdown.get("broker_quality", 0.0)),
        )
        microstructure_score = metadata.get(
            "microstructure_score",
            micro.get("score", breakdown.get("microstructure", 0.0)),
        )
        cross_asset_score = metadata.get(
            "cross_asset_score",
            cross_asset.get("score", breakdown.get("cross_asset", 0.0)),
        )
        item.update(
            {
                "source": "signal",
                "timeframe": str(context.get("timeframe") or ""),
                "memory_score": float(signal.metadata.get("memory_score", 0.0) or 0.0),
                "execution_quality_score": float(signal.metadata.get("execution_quality_score", 0.0) or 0.0),
                "opportunity_score": float(signal.metadata.get("opportunity_score", 0.0) or 0.0),
                "opportunity_rank": int(signal.metadata.get("opportunity_rank", 0) or 0),
                "regime": str(signal.metadata.get("regime", "") or ""),
                "setup_quality": float(signal.metadata.get("setup_quality", 0.0) or 0.0),
                "opportunity_breakdown": dict(breakdown),
                "broker_quality_score": float(broker_quality_score or 0.0),
                "broker_primary_provider": str(broker.get("primary_provider", "") or ""),
                "broker_comparison_provider": str(broker.get("comparison_provider", "") or ""),
                "broker_agreement_state": str(
                    metadata.get("broker_agreement_state", broker.get("quote_agreement_state", "")) or ""
                ),
                "broker_quote_quality_state": str(
                    metadata.get("broker_quote_quality_state", broker.get("quote_quality_state", "")) or ""
                ),
                "broker_spread_regime": str(
                    metadata.get("broker_spread_regime", broker.get("spread_regime", "")) or ""
                ),
                "microstructure_score": float(microstructure_score or 0.0),
                "microstructure_pressure": str(
                    micro.get("pressure_direction", metadata.get("micro_pressure_direction", "")) or ""
                ),
                "depth_available": bool(metadata.get("depth_available", micro.get("depth_available"))),
                "synthetic_depth_available": bool(
                    metadata.get("synthetic_depth_available", micro.get("synthetic_depth_available"))
                ),
                "microstructure_source": str(
                    metadata.get("microstructure_source", micro.get("microstructure_source", "")) or ""
                ),
                "cross_asset_score": float(cross_asset_score or 0.0),
                "cross_asset_alignment": float(metadata.get("cross_asset_alignment", 0.0) or 0.0),
                "cross_asset_confidence": float(metadata.get("cross_asset_confidence", 0.0) or 0.0),
                "cross_asset_state": str(metadata.get("cross_asset_state", cross_asset.get("state", "")) or ""),
                "cross_asset_primary_peer": str(
                    metadata.get("cross_asset_primary_peer", cross_asset.get("dominant_peer", "")) or ""
                ),
                "cross_asset_primary_relation": str(
                    metadata.get("cross_asset_primary_relation", cross_asset.get("dominant_relation", "")) or ""
                ),
                "recent_pattern_block_new_entries": bool(recent_review.get("block_new_entries")),
                "recent_pattern_notes": [str(n) for n in list(recent_review.get("notes") or [])[:4]],
                "recent_pattern_sample_count": int(recent_review.get("sample_count", 0) or 0),
            }
        )
        return item

    def scan_top_ranked_opportunities(self, limit: int = TOP_OPPORTUNITIES_LIMIT) -> List[Dict[str, Any]]:
        if not self.is_ready:
            return []
        signal_ctx_pairs = self._generate_signals()
        accepted_pairs: List[Tuple[Signal, Dict[str, Any]]] = []
        for signal, context in signal_ctx_pairs:
            result = self.decision_engine.evaluate(signal, context)
            if result and result.alive:
                accepted_pairs.append((result, context))
        ranked = self._rank_survivors(accepted_pairs)
        self._remember_ranked_opportunities(ranked)
        return self.get_top_ranked_opportunities(limit=limit, refresh=False)

    def _build_ranked_signal_candidate(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "source": "signal",
            "asset": item.get("asset", ""),
            "category": item.get("category", ""),
            "direction": item.get("direction", ""),
            "confidence": float(item.get("confidence", 0.0) or 0.0),
            "opportunity_score": float(item.get("opportunity_score", 0.0) or 0.0),
            "opportunity_rank": int(item.get("opportunity_rank", 0) or 0),
            "memory_score": float(item.get("memory_score", 0.0) or 0.0),
            "execution_quality_score": float(item.get("execution_quality_score", 0.0) or 0.0),
            "regime": item.get("regime", ""),
            "setup_quality": float(item.get("setup_quality", 0.0) or 0.0),
            "timeframe": item.get("timeframe", ""),
            "strategy_id": item.get("strategy_id", ""),
            "opportunity_breakdown": dict(item.get("opportunity_breakdown") or {}),
            "broker_quality_score": float(item.get("broker_quality_score", 0.0) or 0.0),
            "broker_primary_provider": str(item.get("broker_primary_provider", "") or ""),
            "broker_comparison_provider": str(item.get("broker_comparison_provider", "") or ""),
            "broker_agreement_state": str(item.get("broker_agreement_state", "") or ""),
            "broker_quote_quality_state": str(item.get("broker_quote_quality_state", "") or ""),
            "broker_spread_regime": str(item.get("broker_spread_regime", "") or ""),
            "microstructure_score": float(item.get("microstructure_score", 0.0) or 0.0),
            "microstructure_pressure": str(item.get("microstructure_pressure", "") or ""),
            "depth_available": bool(item.get("depth_available")),
            "synthetic_depth_available": bool(item.get("synthetic_depth_available")),
            "microstructure_source": str(item.get("microstructure_source", "") or ""),
            "cross_asset_score": float(item.get("cross_asset_score", 0.0) or 0.0),
            "cross_asset_alignment": float(item.get("cross_asset_alignment", 0.0) or 0.0),
            "cross_asset_confidence": float(item.get("cross_asset_confidence", 0.0) or 0.0),
            "cross_asset_state": str(item.get("cross_asset_state", "") or ""),
            "cross_asset_primary_peer": str(item.get("cross_asset_primary_peer", "") or ""),
            "cross_asset_primary_relation": str(item.get("cross_asset_primary_relation", "") or ""),
            "recent_pattern_block_new_entries": bool(item.get("recent_pattern_block_new_entries")),
            "recent_pattern_notes": [str(n) for n in list(item.get("recent_pattern_notes") or [])[:4]],
            "recent_pattern_sample_count": int(item.get("recent_pattern_sample_count", 0) or 0),
            "recorded_at_utc": self._last_ranked_at_utc,
        }

    def _build_ranked_position_candidate(self, pos: Dict[str, Any]) -> Dict[str, Any]:
        metrics = self._extract_position_action_metrics(pos)
        meta = dict(pos.get("metadata") or {})
        broker = meta.get("broker_quality") if isinstance(meta.get("broker_quality"), dict) else {}
        micro = meta.get("market_microstructure") if isinstance(meta.get("market_microstructure"), dict) else {}
        cross_asset = meta.get("cross_asset_context") if isinstance(meta.get("cross_asset_context"), dict) else {}
        adaptive_policy = meta.get("adaptive_policy") if isinstance(meta.get("adaptive_policy"), dict) else {}
        recent_review = (
            adaptive_policy.get("recent_review_profile")
            if isinstance(adaptive_policy.get("recent_review_profile"), dict)
            else {}
        )
        breakdown = meta.get("opportunity_breakdown") if isinstance(meta.get("opportunity_breakdown"), dict) else {}
        return {
            "source": "position",
            "trade_id": pos.get("trade_id", ""),
            "asset": pos.get("asset", ""),
            "category": pos.get("category", ""),
            "direction": str(pos.get("direction") or pos.get("signal") or "BUY").upper(),
            "confidence": float(pos.get("confidence", 0.0) or 0.0),
            "opportunity_score": float(metrics.get("opportunity_score", 0.0) or 0.0),
            "opportunity_rank": int(dict(pos.get("metadata") or {}).get("opportunity_rank", 0) or 0),
            "memory_score": float(metrics.get("memory_score", 0.0) or 0.0),
            "execution_quality_score": float(metrics.get("execution_quality_score", 0.0) or 0.0),
            "quality_score": float(metrics.get("quality_score", 0.0) or 0.0),
            "pnl": float(pos.get("pnl", 0.0) or 0.0),
            "opportunity_breakdown": dict(breakdown),
            "broker_quality_score": float(
                meta.get("broker_quality_score", broker.get("score", breakdown.get("broker_quality", 0.0))) or 0.0
            ),
            "broker_primary_provider": str(broker.get("primary_provider", "") or ""),
            "broker_comparison_provider": str(broker.get("comparison_provider", "") or ""),
            "broker_agreement_state": str(
                meta.get("broker_agreement_state", broker.get("quote_agreement_state", "")) or ""
            ),
            "broker_quote_quality_state": str(
                meta.get("broker_quote_quality_state", broker.get("quote_quality_state", "")) or ""
            ),
            "broker_spread_regime": str(meta.get("broker_spread_regime", broker.get("spread_regime", "")) or ""),
            "microstructure_score": float(
                meta.get("microstructure_score", micro.get("score", breakdown.get("microstructure", 0.0))) or 0.0
            ),
            "microstructure_pressure": str(
                micro.get("pressure_direction", meta.get("micro_pressure_direction", "")) or ""
            ),
            "depth_available": bool(meta.get("depth_available", micro.get("depth_available"))),
            "synthetic_depth_available": bool(
                meta.get("synthetic_depth_available", micro.get("synthetic_depth_available"))
            ),
            "microstructure_source": str(
                meta.get("microstructure_source", micro.get("microstructure_source", "")) or ""
            ),
            "cross_asset_score": float(
                meta.get("cross_asset_score", cross_asset.get("score", breakdown.get("cross_asset", 0.0))) or 0.0
            ),
            "cross_asset_alignment": float(meta.get("cross_asset_alignment", 0.0) or 0.0),
            "cross_asset_confidence": float(meta.get("cross_asset_confidence", 0.0) or 0.0),
            "cross_asset_state": str(meta.get("cross_asset_state", cross_asset.get("state", "")) or ""),
            "cross_asset_primary_peer": str(
                meta.get("cross_asset_primary_peer", cross_asset.get("dominant_peer", "")) or ""
            ),
            "cross_asset_primary_relation": str(
                meta.get("cross_asset_primary_relation", cross_asset.get("dominant_relation", "")) or ""
            ),
            "recent_pattern_block_new_entries": bool(recent_review.get("block_new_entries")),
            "recent_pattern_notes": [str(n) for n in list(recent_review.get("notes") or [])[:4]],
            "recent_pattern_sample_count": int(recent_review.get("sample_count", 0) or 0),
            "recorded_at_utc": self._last_ranked_at_utc,
        }

    @staticmethod
    def _ranked_candidate_key(item: Dict[str, Any]) -> Tuple[str, str, str]:
        return (
            str(item.get("source", "") or ""),
            str(item.get("asset", "") or ""),
            str(item.get("direction", "") or ""),
        )

    @staticmethod
    def _ranked_candidate_sort_key(item: Dict[str, Any]) -> Tuple[float, float, float]:
        return (
            float(item.get("opportunity_score", 0.0) or 0.0),
            float(item.get("confidence", 0.0) or 0.0),
            float(item.get("memory_score", 0.0) or 0.0),
        )

    def _dedupe_ranked_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for item in candidates:
            key = self._ranked_candidate_key(item)
            existing = deduped.get(key)
            if existing is None or self._ranked_candidate_sort_key(item) > self._ranked_candidate_sort_key(existing):
                deduped[key] = item
        return sorted(deduped.values(), key=self._ranked_candidate_sort_key, reverse=True)

    def get_top_ranked_opportunities(
        self,
        limit: int = TOP_OPPORTUNITIES_LIMIT,
        refresh: bool = False,
        include_positions: bool = True,
        allow_refresh_when_empty: bool = True,
    ) -> List[Dict[str, Any]]:
        if refresh or (
            allow_refresh_when_empty
            and not self._last_ranked_opportunities
            and self.is_ready
        ):
            try:
                self.scan_top_ranked_opportunities(limit=max(limit, TOP_OPPORTUNITIES_LIMIT))
            except Exception as exc:
                logger.debug(f"[TradingCore] Top opportunity refresh failed: {exc}")

        candidates: List[Dict[str, Any]] = []
        for item in self._last_ranked_opportunities:
            candidates.append(self._build_ranked_signal_candidate(item))

        if include_positions:
            for pos in self.state.get_open_positions():
                candidates.append(self._build_ranked_position_candidate(pos))

        ranked = self._dedupe_ranked_candidates(candidates)
        return ranked[: max(1, int(limit or TOP_OPPORTUNITIES_LIMIT))]

    def _build_signal_generation_context(
        self,
        canonical: str,
        category: str,
        price_data,
    ) -> Tuple[Dict[str, Any], float, float]:
        price = 0.0
        spread = 0.0
        if self.fetcher:
            try:
                price, spread = self.fetcher.get_real_time_price(canonical, category)
                price = price or 0.0
                spread = spread or 0.0
            except Exception:
                pass

        price_meta: Dict[str, Any] = {}
        ohlcv_meta: Dict[str, Any] = {}
        try:
            timeframe = get_trading_timeframe(category)
            if self.fetcher:
                price_meta = self.fetcher.get_last_price_metadata(canonical)
                ohlcv_meta = self.fetcher.get_last_ohlcv_metadata(canonical, timeframe)
        except Exception:
            timeframe = get_trading_timeframe(category)

        ctx = self._build_context(canonical, category)
        ctx["price_data"] = price_data
        ctx["current_price"] = float(price or 0.0)
        ctx["spread"] = spread
        ctx["market_data"] = {"price": price_meta, "ohlcv": ohlcv_meta}
        ctx["timeframe"] = timeframe
        ctx["risk_manager"] = self._risk_manager
        self._attach_cross_asset_context(
            ctx,
            canonical,
            category,
            timeframe=timeframe,
        )
        try:
            if self.fetcher:
                ctx["market_microstructure"] = self.fetcher.get_market_microstructure(
                    canonical,
                    category,
                )
            else:
                ctx["market_microstructure"] = {}
        except Exception:
            ctx["market_microstructure"] = {}
        self._attach_market_structure_context(ctx, canonical, category, price_data)
        self._attach_broker_quality_context(
            ctx,
            canonical,
            category,
            price=price,
            spread=spread,
            price_meta=price_meta,
        )
        try:
            from ml.features import build_features

            features = build_features(price_data)
            if features is not None:
                ctx["features"] = features
        except Exception:
            pass
        return ctx, float(price), float(spread)

    def get_signal_for_asset(self, asset: str) -> Optional[Dict]:
        if not self.is_ready:
            return None
        try:
            canonical = self.registry.canonical(asset)
            category  = self.registry.category(canonical)

            market_open, _reason = self._market_hours_status(canonical, category)
            if not market_open:
                return None

            sig = None
            if self.fetcher:
                try:
                    price_data = self._fetch_price_data(canonical, category)
                    if price_data is not None and not price_data.empty:
                        ctx, _price, _spread = self._build_signal_generation_context(
                            canonical,
                            category,
                            price_data,
                        )
                        sig = self._generate_seed_signal(
                            canonical, canonical, category, price_data, ctx
                        )
                except Exception as _e:
                    logger.debug(f"[TradingCore] get_signal_for_asset seed generate failed for {asset}: {_e}")
                    sig = None

            if sig is None:
                return None

            result = self.decision_engine.evaluate(sig, ctx)
            return result.to_dict() if result else None
        except Exception as e:
            logger.error(f"[TradingCore] get_signal_for_asset({asset}): {e}")
            return None

    def inspect_asset(self, asset: str) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "asset": asset,
            "canonical_asset": asset,
            "category": "",
            "market_status": {"market_open": False, "reason": "engine_not_ready"},
            "decision_status": "engine_not_ready",
            "decision_reason": "engine_not_ready",
            "signal": None,
            "journal_text": "",
            "journal_summary": {},
            "playbook_decision": {},
            "seed_decision": {},
            "market_structure": {},
            "market_intelligence": {},
            "broker_quality": {},
            "market_microstructure": {},
            "cross_asset_context": {},
            "sentiment_score": 0.0,
            "funding_bias": "NEUTRAL",
            "oi_signal": "NEUTRAL",
            "timeframe": "",
            "current_price": 0.0,
            "latest_close": 0.0,
            "open_position": None,
        }
        if not self.is_ready:
            return result
        try:
            canonical = self.registry.canonical(asset)
            category = self.registry.category(canonical)
            market_open, market_reason = self._market_hours_status(canonical, category)
            result.update(
                {
                    "asset": canonical,
                    "canonical_asset": canonical,
                    "category": category,
                    "market_status": {"market_open": bool(market_open), "reason": str(market_reason or "")},
                }
            )

            open_position = next(
                (
                    dict(pos)
                    for pos in self.state.get_open_positions()
                    if self.registry.canonical(str(pos.get("asset", "") or "")) == canonical
                ),
                None,
            )
            result["open_position"] = open_position

            price_data = self._fetch_price_data(canonical, category)
            if price_data is None or price_data.empty:
                result["decision_status"] = "no_price_data"
                result["decision_reason"] = "no_price_data"
                return result

            try:
                result["latest_close"] = float(price_data["close"].iloc[-1])
            except Exception:
                result["latest_close"] = 0.0

            ctx, price, _spread = self._build_signal_generation_context(canonical, category, price_data)
            ctx["_inspection_only"] = True

            result.update(
                {
                    "market_intelligence": dict(ctx.get("market_intelligence") or {}),
                    "market_structure": dict(ctx.get("market_structure") or {}),
                    "broker_quality": dict(ctx.get("broker_quality") or {}),
                    "market_microstructure": dict(ctx.get("market_microstructure") or {}),
                    "cross_asset_context": dict(ctx.get("cross_asset_context") or {}),
                    "sentiment_score": float(ctx.get("sentiment_score", 0.0) or 0.0),
                    "funding_bias": str(ctx.get("funding_bias", "NEUTRAL") or "NEUTRAL"),
                    "oi_signal": str(ctx.get("oi_signal", "NEUTRAL") or "NEUTRAL"),
                    "timeframe": str(ctx.get("timeframe") or ""),
                    "current_price": float(price or result.get("latest_close") or 0.0),
                }
            )

            seed_signal = self._generate_seed_signal(
                canonical,
                canonical,
                category,
                price_data,
                ctx,
            )
            result["playbook_decision"] = dict(ctx.get("playbook_decision") or {})
            result["seed_decision"] = dict(ctx.get("seed_decision") or {})

            if seed_signal is None:
                blocked_reason = str(
                    result["seed_decision"].get("reason")
                    or result["playbook_decision"].get("blocked_reason")
                    or ("market_closed" if not market_open else "no_playbook_seed")
                ).strip() or ("market_closed" if not market_open else "no_playbook_seed")
                result["decision_status"] = "market_closed" if not market_open else "no_seed"
                result["decision_reason"] = blocked_reason
                return result

            preview = self.decision_engine.preview(seed_signal, ctx)
            result["signal"] = preview.to_dict()
            result["journal_summary"] = dict(preview.journal.summary(preview) or {})
            result["journal_text"] = preview.journal.to_telegram_plain(preview)
            result["decision_status"] = "accepted" if preview.alive else "killed"
            result["decision_reason"] = str(preview.kill_reason or "").strip()
            return result
        except Exception as e:
            logger.error(f"[TradingCore] inspect_asset({asset}): {e}")
            result["decision_status"] = "error"
            result["decision_reason"] = str(e)
            return result

    def set_cooldown(self, asset: str, minutes: int = 60) -> None:
        canonical = self.registry.canonical(asset)
        self.state.set_cooldown(canonical, minutes)

    def get_cooldowns(self) -> Dict[str, int]:
        return self.state.get_all_cooldowns()

    def subscribe(self, event_type: Type, callback: Callable, async_dispatch: bool = True) -> None:
        self.events.subscribe(event_type, callback, async_dispatch=async_dispatch)

    @staticmethod
    def _monitor_health_snapshot() -> Dict[str, Any]:
        source_health: Dict[str, Any] = {}
        stale_sources: List[str] = []
        never_seen_sources: List[str] = []
        recent_error_count = 0
        recent_errors: List[Dict[str, Any]] = []
        try:
            from monitoring.system_health_service import monitor as _mon

            monitor_snapshot = _mon.get_snapshot()
            source_health = dict(monitor_snapshot.get("source_health") or {})
            stale_sources = sorted(
                [
                    name
                    for name, health in source_health.items()
                    if isinstance(health, dict) and str(health.get("status") or "") == "stale"
                ]
            )
            never_seen_sources = sorted(
                [
                    name
                    for name, health in source_health.items()
                    if isinstance(health, dict) and str(health.get("status") or "") == "never_seen"
                ]
            )
            recent_error_count = int(monitor_snapshot.get("recent_error_count", 0) or 0)
            recent_errors = list(monitor_snapshot.get("recent_errors") or [])[-5:]
        except Exception:
            pass
        return {
            "source_health": source_health,
            "stale_sources": stale_sources,
            "never_seen_sources": never_seen_sources,
            "recent_error_count": recent_error_count,
            "recent_errors": recent_errors,
        }

    @staticmethod
    def _ig_broker_health_snapshot() -> Dict[str, Any]:
        try:
            from services.market_data_router import get_broker_account_summary

            return dict(get_broker_account_summary() or {})
        except Exception:
            return {}

    def _build_signal_diagnostics_snapshot(self, positions: List[Dict[str, Any]]) -> Dict[str, Any]:
        total = 0
        broker_fragile = 0
        broker_supportive = 0
        true_depth = 0
        synthetic_depth = 0
        cross_conflict = 0
        cross_support = 0
        recent_pattern_blocks = 0
        for position in positions:
            meta = dict(position.get("metadata") or {})
            broker = meta.get("broker_quality") if isinstance(meta.get("broker_quality"), dict) else {}
            micro = meta.get("market_microstructure") if isinstance(meta.get("market_microstructure"), dict) else {}
            cross = meta.get("cross_asset_context") if isinstance(meta.get("cross_asset_context"), dict) else {}
            adaptive = meta.get("adaptive_policy") if isinstance(meta.get("adaptive_policy"), dict) else {}
            recent_review = (
                adaptive.get("recent_review_profile")
                if isinstance(adaptive.get("recent_review_profile"), dict)
                else {}
            )
            total += 1

            agreement_state = str(broker.get("quote_agreement_state") or "").lower()
            spread_regime = str(broker.get("spread_regime") or "").lower()
            quote_quality_state = str(broker.get("quote_quality_state") or "").lower()
            broker_score = float(meta.get("broker_quality_score", broker.get("score", 0.0)) or 0.0)
            transition_risk = float(broker.get("market_transition_risk", 0.0) or 0.0)
            if (
                agreement_state in {"divergent", "severe_divergence"}
                or spread_regime in {"stressed", "extreme", "wide"}
                or quote_quality_state in {"stale", "delayed"}
                or transition_risk >= 0.65
                or bool(broker.get("market_state_changed"))
            ):
                broker_fragile += 1
            elif (
                broker_score >= 0.65
                and agreement_state in {"strong", "aligned"}
                and spread_regime in {"tight", "normal"}
                and quote_quality_state in {"fresh", "aging"}
            ):
                broker_supportive += 1

            if bool(micro.get("depth_available")):
                true_depth += 1
            elif bool(micro.get("synthetic_depth_available")):
                synthetic_depth += 1

            cross_alignment = float(
                meta.get("cross_asset_alignment", cross.get("alignment", cross.get("score", 0.0))) or 0.0
            )
            cross_confidence = float(meta.get("cross_asset_confidence", cross.get("confidence", 0.0)) or 0.0)
            if cross_confidence >= 0.20 and cross_alignment <= -0.20:
                cross_conflict += 1
            elif cross_confidence >= 0.20 and cross_alignment >= 0.20:
                cross_support += 1

            if bool(recent_review.get("block_new_entries")):
                recent_pattern_blocks += 1

        summary_parts = [
            f"Fragile {broker_fragile}" if broker_fragile else "",
            f"True depth {true_depth}" if true_depth else "",
            f"Synthetic {synthetic_depth}" if synthetic_depth else "",
            f"Cross conflicts {cross_conflict}" if cross_conflict else "",
            f"Pattern blocks {recent_pattern_blocks}" if recent_pattern_blocks else "",
        ]
        return {
            "count": total,
            "broker_fragile_count": broker_fragile,
            "broker_supportive_count": broker_supportive,
            "true_depth_count": true_depth,
            "synthetic_depth_count": synthetic_depth,
            "cross_conflict_count": cross_conflict,
            "cross_support_count": cross_support,
            "recent_pattern_block_count": recent_pattern_blocks,
            "summary_label": " · ".join([part for part in summary_parts if part]) or "No active diagnostics",
        }

    def health_report(self) -> Dict:
        try:
            import psutil
            ram = psutil.virtual_memory().percent
            cpu = psutil.cpu_percent(interval=0)
        except Exception:
            ram = cpu = 0.0
        issues = [] if self._engine_ready.is_set() else ["Engine initialising"]
        monitor_health = self._monitor_health_snapshot()
        source_health = dict(monitor_health["source_health"])
        stale_sources = list(monitor_health["stale_sources"])
        never_seen_sources = list(monitor_health["never_seen_sources"])
        recent_error_count = int(monitor_health["recent_error_count"])
        recent_errors = list(monitor_health["recent_errors"])
        ig_broker = self._ig_broker_health_snapshot()
        signal_diagnostics: Dict[str, Any] = {}
        try:
            positions = list(self.get_positions() or [])
            signal_diagnostics = self._build_signal_diagnostics_snapshot(positions)
        except Exception:
            signal_diagnostics = {}
        if stale_sources:
            issues.append(f"Stale data sources: {', '.join(stale_sources[:4])}")
        if recent_error_count > 0:
            issues.append(f"Recent monitor errors: {recent_error_count}")
        if ig_broker.get("enabled") and not ig_broker.get("authenticated", False):
            error_message = str(ig_broker.get("error_message") or ig_broker.get("error_code") or "unavailable")
            issues.append(f"IG broker data unavailable: {error_message}")
        return {
            "is_running":       self._is_running,
            "engine_ready":     self._engine_ready.is_set(),
            "strategy_mode":    self.strategy_mode,
            "balance":          self.state.balance,
            "open_positions":   self.state.open_position_count(),
            "daily_trades":     self.state.daily_trades,
            "daily_pnl":        self.state.daily_pnl,
            "active_cooldowns": len(self.state.get_all_cooldowns()),
            "ram_pct":          ram,
            "cpu_pct":          cpu,
            "source_health":    source_health,
            "stale_sources":    stale_sources,
            "stale_source_count": len(stale_sources),
            "never_seen_sources": never_seen_sources,
            "never_seen_source_count": len(never_seen_sources),
            "recent_error_count": recent_error_count,
            "recent_errors":    recent_errors,
            "ig_broker":        ig_broker,
            "signal_diagnostics": signal_diagnostics,
            "issues":           issues,
            "status":           "healthy" if not issues else "degraded",
        }

    def _record_trade_close_side_effects(self, trade: Dict[str, Any], pnl: float) -> None:
        if self._risk_manager is not None:
            self._risk_manager.update_balance(self.state.balance)
        self._notify_telegram_close(trade)
        try:
            from services.personality_service import personality as _personality

            _personality.record_trade(trade)
        except Exception:
            pass
        try:
            from monitoring.system_health_service import monitor as _mon

            _mon.record_trade_result(pnl)
        except Exception:
            pass

    @staticmethod
    def _resolve_post_close_cooldown_minutes(trade: Optional[Dict[str, Any]] = None) -> int:
        metadata = {}
        if isinstance((trade or {}).get("metadata"), dict):
            metadata = dict((trade or {}).get("metadata") or {})
        adaptive_policy = metadata.get("adaptive_policy") if isinstance(metadata.get("adaptive_policy"), dict) else {}
        adaptive_minutes = adaptive_policy.get("cooldown_minutes")
        try:
            adaptive_value = int(float(adaptive_minutes or 0))
        except Exception:
            adaptive_value = 0
        return adaptive_value if adaptive_value > 0 else int(TRADE_CLOSE_COOLDOWN_MINUTES)

    def _set_post_close_cooldown(self, asset: str, trade: Optional[Dict[str, Any]] = None) -> None:
        try:
            canonical = self.registry.canonical(asset)
            cooldown_minutes = self._resolve_post_close_cooldown_minutes(trade)
            self.state.set_cooldown(canonical, cooldown_minutes)
            logger.info(
                f"[TradingCore] Set cooldown {cooldown_minutes}m "
                f"for {canonical} after close"
            )
        except Exception:
            pass

    def _handle_partial_close_callback(self, trade: Dict[str, Any]) -> None:
        trade_id = str(trade.get("trade_id", "") or "")
        parent_trade_id = str(trade.get("parent_trade_id", "") or "")
        exit_reason = str(trade.get("exit_reason", "Unknown") or "Unknown")
        pnl = float(trade.get("pnl", 0.0) or 0.0)

        recorded = self.state.record_partial_close(parent_trade_id, trade)
        if recorded is None:
            return

        self._record_trade_close_side_effects(recorded, pnl)
        logger.log_trade(
            "PARTIAL_CLOSE",
            trade_id=trade_id,
            parent_trade_id=parent_trade_id,
            asset=recorded.get("asset", ""),
            pnl=round(pnl, 4),
            reason=exit_reason,
        )

    def _handle_full_close_callback(self, trade: Dict[str, Any]) -> None:
        trade_id = str(trade.get("trade_id", "") or "")
        exit_price = float(trade.get("exit_price", 0.0) or 0.0)
        exit_reason = str(trade.get("exit_reason", "Unknown") or "Unknown")
        pnl = float(trade.get("pnl", 0.0) or 0.0)
        close_updates = {
            "pnl_percent": trade.get("pnl_percent"),
            "highest_price": trade.get("highest_price"),
            "lowest_price": trade.get("lowest_price"),
            "tp_hit": trade.get("tp_hit"),
            "risk_reward": trade.get("risk_reward"),
            "original_sl": trade.get("original_sl"),
            "original_take_profit": trade.get("original_take_profit"),
            "metadata": trade.get("metadata"),
        }
        closed = self.state.close_position(
            trade_id,
            exit_price,
            exit_reason,
            pnl,
            extra_updates=close_updates,
        )
        if closed is None:
            logger.debug(f"[TradingCore] Ignoring duplicate close callback for {trade_id}")
            return

        try:
            from services.dom_replay_service import get_service as get_dom_replay_service

            get_dom_replay_service().attach_trade_outcome(closed)
        except Exception as exc:
            logger.debug(f"[TradingCore] DOM replay close capture failed: {exc}")

        self._record_trade_close_side_effects(closed, pnl)
        logger.log_trade(
            "CLOSE",
            trade_id=trade_id,
            asset=closed.get("asset", ""),
            pnl=round(pnl, 4),
            reason=exit_reason,
        )
        self._set_post_close_cooldown(str(closed.get("asset", "") or ""), closed)

    def _handle_trade_closed_callback(self, trade: Dict[str, Any]) -> None:
        try:
            if trade.get("is_partial_close"):
                self._handle_partial_close_callback(trade)
                return
            self._handle_full_close_callback(trade)
        except Exception as e:
            logger.error(f"[TradingCore] on_trade_closed error: {e}")

    def _handle_position_updated_callback(self, position: Dict[str, Any]) -> None:
        try:
            self.state.sync_open_position(position)
        except Exception as e:
            logger.error(f"[TradingCore] on_position_updated error: {e}")

    def _restore_paper_trader_positions(self) -> None:
        if self._paper_trader is None:
            return
        for pos in self.state.get_open_positions():
            self._paper_trader.restore_position(pos)

    def _start_ohlcv_prewarm(self) -> None:
        def _prewarm() -> None:
            try:
                asset_list = self.registry.all_assets()
                for canonical, category in asset_list:
                    if self._stop_event.is_set():
                        break
                    try:
                        self.fetcher.get_ohlcv(canonical, category)
                    except Exception:
                        pass
                logger.info("[TradingCore] OHLCV cache pre-warmed")
            except Exception as e:
                logger.debug(f"[TradingCore] Pre-warm error: {e}")

        threading.Thread(target=_prewarm, name="ohlcv-prewarm", daemon=True).start()

    def _emit_position_update_event(self) -> None:
        try:
            from core.events import PositionUpdateEvent

            self.events.emit(
                PositionUpdateEvent(
                    open_positions=self.state.get_open_positions(),
                    balance=self.state.balance,
                    daily_pnl=self.state.daily_pnl,
                    daily_trades=self.state.daily_trades,
                )
            )
        except Exception:
            pass

    def _initialize_runtime_components(self) -> None:
        from data.fetcher import DataFetcher
        from execution.paper_trader import PaperTrader
        from risk.manager import RiskManager

        self.fetcher = DataFetcher()
        self._risk_manager = RiskManager(account_balance=self.state.balance)
        self._strategy = None
        self._predictor = None
        self._agent = None
        logger.info("[TradingCore] Playbook-native runtime active — legacy policy agent removed")
        self._paper_trader = PaperTrader(
            account_balance=self.state.balance,
            risk_manager=self._risk_manager,
        )

    def _configure_paper_trader_callbacks(self) -> None:
        if self._paper_trader is None:
            return
        self._paper_trader.on_trade_closed = self._handle_trade_closed_callback
        self._paper_trader.on_position_updated = self._handle_position_updated_callback

    @staticmethod
    def _cycle_wait_duration(elapsed: float, scan_interval: float) -> float:
        return max(5.0, float(scan_interval) - float(elapsed))

    def _run_cycle_once(self) -> float:
        cycle_start = time.monotonic()
        try:
            self._trading_cycle()
        except Exception as e:
            logger.error(f"[TradingCore] Cycle error: {e}", exc_info=True)
        self._emit_position_update_event()
        return time.monotonic() - cycle_start

    @staticmethod
    def _market_hours_status(asset: str, category: str) -> Tuple[bool, str]:
        try:
            from services.market_data_router import get_market_status

            status = get_market_status(asset, category=category)
            if status and "market_open" in status:
                try:
                    from services.market_hours_guard import build_market_status

                    normalized = build_market_status(asset, category, provider_status=status)
                    return bool(normalized["market_open"]), str(normalized.get("reason", "market status"))
                except Exception:
                    return bool(status["market_open"]), str(status.get("reason", "market status"))
        except Exception:
            pass
        return TradingCore._market_hours_status_fallback(asset, category)

    # ── Internal — init ───────────────────────────────────────────────────────

    def _init_subsystems(self) -> bool:
        try:
            self._initialize_runtime_components()
            self._configure_paper_trader_callbacks()
            # ──────────────────────────────────────────────────────────────────

            self._restore_paper_trader_positions()

            # ── Offline gap-fill check ────────────────────────────────────────
            # For every restored position, scan OHLCV history from open_time
            # to now and close any position whose SL or TP was breached while
            # the bot was offline. First breach chronologically wins.
            self._check_offline_sl_tp()
            flattened = self._flatten_positions_before_close()
            if flattened:
                logger.info(
                    f"[TradingCore] Pre-close flatten closed {flattened} position(s) during init"
                )

            logger.info("[TradingCore] Playbook-native runtime active — ML registry load removed")

            # Pre-warm OHLCV cache in background — avoids slow first trading cycle
            self._start_ohlcv_prewarm()

            self._engine_ready.set()
            logger.info("[TradingCore] All subsystems ready")
            return True

        except Exception as e:
            logger.error(f"[TradingCore] Subsystem init failed: {e}", exc_info=True)
            return False

    # ── Internal — main loop ──────────────────────────────────────────────────

    def _run(self) -> None:
        if not self._init_subsystems():
            logger.error("[TradingCore] Init failed — loop exiting")
            self._is_running = False
            return

        logger.info("[TradingCore] Entering trading loop")
        from config.config import SCAN_INTERVAL_SECONDS

        while not self._stop_event.is_set():
            elapsed = self._run_cycle_once()
            wait = self._cycle_wait_duration(elapsed, SCAN_INTERVAL_SECONDS)
            self._stop_event.wait(timeout=wait)

        logger.info("[TradingCore] Loop exited")

    def _refresh_live_positions(self) -> None:
        if self._paper_trader is None:
            return
        try:
            prices = self._get_prices()
            self._paper_trader.update_positions(prices)
            try:
                from redis_broker import broker as _redis_broker

                for asset, price in prices.items():
                    cat = next(
                        (
                            p.get("category", "forex")
                            for p in self.state.get_open_positions()
                            if p.get("asset") == asset
                        ),
                        "forex",
                    )
                    _redis_broker.publish_price(asset, price, cat)
            except Exception:
                pass
        except Exception as e:
            logger.error(f"[TradingCore] Position update error: {e}")

    def _flatten_positions_before_close(self) -> int:
        positions = list(self.state.get_open_positions() or [])
        if not positions:
            return 0

        try:
            from services.market_data_router import get_market_status
        except Exception:
            get_market_status = None

        try:
            from services.market_hours_guard import build_market_status
        except Exception:
            return 0

        closable_trade_ids: List[Tuple[str, str]] = []
        for pos in positions:
            asset = str(pos.get("asset", "") or "")
            category = str(pos.get("category", "") or "")
            trade_id = str(pos.get("trade_id", "") or "")
            if not trade_id or not asset:
                continue
            status = None
            if get_market_status is not None:
                try:
                    status = get_market_status(asset, category=category)
                except Exception:
                    status = None
            if not isinstance(status, dict) or "close_buffer_active" not in status:
                status = build_market_status(asset, category)
            if bool(status.get("close_buffer_active")):
                closable_trade_ids.append((trade_id, str(status.get("close_buffer_reason") or status.get("reason") or "")))

        if not closable_trade_ids:
            return 0

        logger.info(
            "[TradingCore] Pre-close flattening %d position(s): %s",
            len(closable_trade_ids),
            ", ".join(trade_id for trade_id, _reason in closable_trade_ids[:8]),
        )

        closed_count = 0
        for trade_id, reason in closable_trade_ids:
            try:
                closed = self.close_position_manually(trade_id, reason="Pre-close flatten")
                if closed is not None:
                    closed_count += 1
            except Exception as exc:
                logger.warning(
                    f"[TradingCore] Pre-close flatten failed for {trade_id}: {exc}"
                    + (f" ({reason})" if reason else "")
                )
        return closed_count

    def _evaluate_signal_contexts(
        self,
        signal_ctx_pairs: List[Tuple[Signal, Dict[str, Any]]],
    ) -> List[Tuple[Signal, Dict[str, Any]]]:
        accepted_pairs: List[Tuple[Signal, Dict[str, Any]]] = []
        for sig, ctx in signal_ctx_pairs:
            result = self.decision_engine.evaluate(sig, ctx)
            if result is not None:
                accepted_pairs.append((result, ctx))
            else:
                self._log_decision_rejection(sig, ctx)
        return accepted_pairs

    def _publish_ranked_survivors(
        self,
        ranked_pairs: List[Tuple[Signal, Dict[str, Any]]],
    ) -> None:
        for result, _ctx in ranked_pairs:
            try:
                from redis_broker import broker as _redis_broker

                _redis_broker.publish_signal(result.to_dict())
            except Exception:
                pass

    @staticmethod
    def _select_execution_survivors(
        survivors: List[Signal],
        limit: int = 3,
    ) -> List[Signal]:
        max_count = max(1, int(limit or 1))
        return survivors[:max_count]

    def _execute_ranked_survivors(self, survivors: List[Signal], limit: int = 3) -> int:
        selected_survivors = self._filter_broker_supported_survivors(
            self._select_execution_survivors(survivors, limit=limit)
        )
        if selected_survivors:
            selection_summary = ", ".join(
                f"{sig.asset}({sig.category})#{sig.metadata.get('opportunity_rank', '?')}"
                for sig in selected_survivors
            )
            logger.info(f"[TradingCore] Execution selection: {selection_summary}")
        elif survivors:
            logger.info("[TradingCore] No accepted signals could be sent to the active broker right now")
        processed = 0
        for sig in selected_survivors:
            if self._stop_event.is_set():
                break
            if sig.confidence < TRADE_MIN_CONFIDENCE:
                logger.info(
                    f"[TradingCore] Skipping execution for {sig.asset} due to confidence "
                    f"{sig.confidence:.3f} < {TRADE_MIN_CONFIDENCE}"
                )
                continue
            if self._execute_signal(sig):
                processed += 1
        return processed

    def _trading_cycle(self) -> None:
        self._sync_broker_account_balance()

        # Day rollover — reset risk guard to today's opening balance (Issue 6)
        rolled = self.state.check_day_rollover()
        if rolled and self._risk_manager:
            self._risk_manager.reset_daily(self.state.balance)
            logger.info(
                f"[TradingCore] New trading day — risk guard reset "
                f"at ${self.state.balance:.2f}"
            )

        self._refresh_live_positions()
        flattened = self._flatten_positions_before_close()
        if flattened:
            logger.info(f"[TradingCore] Pre-close flatten closed {flattened} position(s)")

        # Generate signals with per-asset contexts (Issues 2 & 9)
        signal_ctx_pairs = self._generate_signals()
        if not signal_ctx_pairs:
            return

        # Run each signal through the decision engine with its own context
        accepted_pairs = self._evaluate_signal_contexts(signal_ctx_pairs)
        ranked_pairs = self._rank_survivors(accepted_pairs)
        survivors = [sig for sig, _ctx in ranked_pairs]
        self._publish_ranked_survivors(ranked_pairs)

        logger.info(
            f"[TradingCore] {len(signal_ctx_pairs)} signals → "
            f"{len(survivors)} accepted"
        )

        processed = self._execute_ranked_survivors(survivors, limit=3)
        if processed:
            logger.info(f"[TradingCore] Executed {processed} trade(s)")

    def _rank_survivors(
        self,
        accepted_pairs: List[Tuple[Signal, Dict[str, Any]]],
    ) -> List[Tuple[Signal, Dict[str, Any]]]:
        if not accepted_pairs:
            self._remember_ranked_opportunities([])
            return []
        try:
            from services.opportunity_ranker import get_service as get_opportunity_ranker

            ranked_pairs = get_opportunity_ranker().rank(accepted_pairs, self.state)
            self._remember_ranked_opportunities(ranked_pairs)
            if ranked_pairs:
                ranking_summary = ", ".join(
                    f"{sig.asset}#{sig.metadata.get('opportunity_rank', '?')}={sig.metadata.get('opportunity_score', 0):.3f}"
                    for sig, _ctx in ranked_pairs[:5]
                )
                logger.info(f"[TradingCore] Opportunity ranking: {ranking_summary}")
            return ranked_pairs
        except Exception as exc:
            logger.debug(f"[TradingCore] Opportunity ranking failed: {exc}")
            self._remember_ranked_opportunities(accepted_pairs)
        return accepted_pairs

    def _filter_broker_supported_survivors(self, survivors: List[Signal]) -> List[Signal]:
        router = getattr(self, "exchange_router", None)
        if router is None:
            return survivors
        filtered: List[Signal] = []
        for sig in survivors:
            payload = {
                "asset": sig.asset,
                "symbol": sig.asset,
                "category": sig.category,
            }
            try:
                supported, reason = router.check_support(payload)
            except Exception as exc:
                supported, reason = False, str(exc)
            if supported:
                filtered.append(sig)
                continue
            reason_text = str(reason or "")
            if reason_text.startswith("broker_temporarily_unavailable"):
                logger.warning(
                    f"[TradingCore] Deferring {sig.asset}: active broker temporarily unavailable"
                    + (f" ({reason_text})" if reason_text else "")
                )
                self._notify_broker_issue(
                    sig.asset,
                    sig.category,
                    reason_text,
                    stage="broker support check",
                    action="deferred until broker API recovers; other assets continue",
                )
            else:
                logger.warning(
                    f"[TradingCore] Skipping {sig.asset}: active broker cannot execute it"
                    + (f" ({reason_text})" if reason_text else "")
                )
                self._notify_broker_issue(
                    sig.asset,
                    sig.category,
                    reason_text,
                    stage="broker support check",
                    action="skipped this asset for this scan; other assets continue",
                )
        return filtered

    @staticmethod
    def _broker_balance_enabled() -> bool:
        mode = str(EXECUTION_MODE or "paper").lower()
        return bool(IG_EXECUTION_ENABLED or mode in {"ig", "ig_demo", "ig_live"})

    def _sync_broker_account_balance(self, *, force: bool = False) -> None:
        if not self._broker_balance_enabled():
            return
        now = time.monotonic()
        if not force and now - float(self._last_broker_balance_sync or 0.0) < 30.0:
            return
        self._last_broker_balance_sync = now
        try:
            from services.market_data_router import get_broker_account_balance_summary

            summary = dict(get_broker_account_balance_summary() or {})
            self._last_broker_balance_snapshot = summary
            if not summary.get("authenticated"):
                return
            balance = summary.get("balance")
            if balance is None:
                return
            broker_balance = float(balance)
            if broker_balance <= 0:
                return
            current = float(self.state.balance or 0.0)
            if abs(current - broker_balance) < 0.01:
                return
            sync_fn = getattr(self.state, "sync_balance", None)
            if callable(sync_fn):
                sync_fn(broker_balance, "ig_account")
            else:
                self.state.adjust_balance(broker_balance - current)
            if self._risk_manager is not None:
                self._risk_manager.update_balance(broker_balance)
            if self._paper_trader is not None:
                try:
                    self._paper_trader.account_balance = broker_balance
                except Exception:
                    pass
            logger.info(
                f"[TradingCore] Synced IG {summary.get('environment', 'demo')} balance "
                f"{summary.get('currency') or ''} {broker_balance:,.2f}"
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Broker balance sync skipped: {exc}")

    def _classify_signal_candidates(
        self,
        asset_list: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
        base_candidates: List[Tuple[str, str]] = []
        tradable_candidates: List[Tuple[str, str]] = []
        market_block_counts: Counter[str] = Counter()
        cooling_count = 0
        open_position_count = self.state.open_position_count()
        addon_candidate_count = 0
        for canonical, category in asset_list:
            if self.state.is_cooling_down(canonical):
                cooling_count += 1
                continue
            if self.state.has_open_position_for(canonical):
                if not self._winner_addon_scan_allowed(canonical):
                    continue
                addon_candidate_count += 1
            base_candidates.append((canonical, category))
            market_open, block_reason = self._market_hours_status(canonical, category)
            if market_open:
                tradable_candidates.append((canonical, category))
            else:
                market_block_counts[block_reason] += 1
        return {
            "base_candidates": base_candidates,
            "tradable_candidates": tradable_candidates,
            "market_block_counts": market_block_counts,
            "cooling_count": cooling_count,
            "open_position_count": open_position_count,
            "addon_candidate_count": addon_candidate_count,
        }

    def _log_signal_scan_overview(
        self,
        *,
        asset_total: int,
        candidate_total: int,
        tradable_total: int,
        cooling_count: int,
        open_position_count: int,
        market_closed_count: int,
    ) -> None:
        logger.debug(f"[TradingCore] Starting signal generation for {tradable_total} tradable candidates")
        logger.info(
            f"[TradingCore] Asset scan: total={asset_total} candidates={candidate_total} "
            f"tradable_now={tradable_total} "
            f"cooling={cooling_count} "
            f"open_pos={open_position_count} "
            f"market_closed={market_closed_count}"
        )

    def _log_empty_signal_scan(
        self,
        market_block_counts: Counter[str],
    ) -> None:
        if market_block_counts:
            blocked = ", ".join(
                f"{reason}={count}" for reason, count in sorted(market_block_counts.items())
            )
            logger.info(
                f"[TradingCore] Signal scan summary: generated=0 "
                f"(all candidates blocked by market hours: {blocked})"
            )
        else:
            logger.info("[TradingCore] Signal scan summary: generated=0 (no candidates available)")

    def _generate_signal_for_asset_task(
        self,
        candidate: Tuple[str, str],
    ) -> Tuple[str, Any]:
        canonical, category = candidate
        if self._stop_event.is_set():
            return ("stopped", None)

        try:
            price_data = self._fetch_price_data(canonical, category)
            if price_data is None or price_data.empty:
                logger.debug(f"[TradingCore] {canonical}: no price data")
                return ("no_price_data", canonical)

            ctx, _price, _spread = self._build_signal_generation_context(
                canonical,
                category,
                price_data,
            )

            sig = self._generate_seed_signal(
                canonical, canonical, category, price_data, ctx
            )
            if sig:
                if self.state.has_open_position_for(canonical):
                    addon_plan = self._build_winner_addon_plan(
                        canonical,
                        category,
                        sig,
                        float(ctx.get("current_price", 0.0) or sig.entry_price or 0.0),
                    )
                    if not addon_plan:
                        logger.debug(f"[TradingCore] {canonical}: open position active but no valid winner add-on")
                        return ("no_addon_signal", canonical)
                    sig.metadata["winner_addon"] = dict(addon_plan)
                    ctx["winner_addon"] = dict(addon_plan)
                logger.info(
                    f"[TradingCore] CANDIDATE: {canonical} {sig.direction} "
                    f"seed_score={sig.confidence:.3f} "
                    f"entry_style={str(sig.metadata.get('playbook_entry_style') or 'n/a').lower()} "
                    f"generic_flow_override={str(sig.metadata.get('generic_flow_override_source') or 'off').lower()} "
                    f"session={str(sig.metadata.get('session_label') or sig.metadata.get('playbook_session') or 'n/a').lower()} "
                    f"ctx={self._fmt_metric(sig.metadata.get('playbook_context_confluence'))} "
                    f"cross={self._fmt_metric(sig.metadata.get('playbook_cross_alignment'))} "
                    f"micro={self._fmt_metric(sig.metadata.get('playbook_micro_score'))} "
                    f"support={int(sig.metadata.get('playbook_support_components', 0) or 0)} "
                    f"conflict={int(sig.metadata.get('playbook_conflict_components', 0) or 0)}"
                )
                return ("signal", (sig, ctx))
            logger.debug(f"[TradingCore] {canonical}: no seed signal generated")
            return ("no_seed_signal", canonical)
        except Exception as e:
            logger.warning(f"[TradingCore] Signal gen {canonical}: {e}")
            return ("error", canonical)

    def _collect_generated_signals(
        self,
        candidates: List[Tuple[str, str]],
    ) -> Tuple[List[Tuple[Signal, Dict]], Counter[str], int]:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from config.config import MAX_SCAN_WORKERS

        result: List[Tuple[Signal, Dict]] = []
        status_counts: Counter[str] = Counter()
        task_count = 0
        max_workers = max(1, min(int(MAX_SCAN_WORKERS or 1), len(candidates) or 1))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(self._generate_signal_for_asset_task, candidate): candidate for candidate in candidates}
            task_count = len(futures)
            logger.debug(
                f"[TradingCore] Submitted {task_count} asset tasks to thread pool "
                f"(max_workers={max_workers})"
            )
            for future in as_completed(futures):
                if self._stop_event.is_set():
                    break
                try:
                    status, payload = future.result()
                    if status == "signal" and payload is not None:
                        result.append(payload)
                        logger.debug(f"[TradingCore] Got signal from future: {payload[0].asset}")
                    else:
                        status_counts[status] += 1
                        logger.debug(
                            f"[TradingCore] Future status {status} for {futures.get(future, 'unknown')}"
                        )
                except Exception as e:
                    asset_pair = futures.get(future, "unknown")
                    status_counts["future_error"] += 1
                    logger.error(f"[TradingCore] Future failed for {asset_pair}: {e}")
        return result, status_counts, task_count

    def _log_signal_scan_summary(
        self,
        *,
        tradable_count: int,
        generated_count: int,
        status_counts: Counter[str],
        market_block_counts: Counter[str],
        task_count: int,
    ) -> None:
        summary_parts = [
            f"tradable={tradable_count}",
            f"generated={generated_count}",
            f"no_edge={status_counts.get('no_seed_signal', 0)}",
            f"no_addon={status_counts.get('no_addon_signal', 0)}",
            f"no_price={status_counts.get('no_price_data', 0)}",
            f"market_closed={status_counts.get('market_closed', 0)}",
            f"errors={status_counts.get('error', 0) + status_counts.get('future_error', 0)}",
        ]
        if market_block_counts and generated_count == 0:
            block_detail = ", ".join(
                f"{reason}={count}" for reason, count in sorted(market_block_counts.items())
            )
            summary_parts.append(f"blocked_by={block_detail}")
        logger.info(f"[TradingCore] Signal scan summary: {' '.join(summary_parts)}")
        logger.debug(
            f"[TradingCore] Signal generation complete: {generated_count} signals generated "
            f"from {task_count} tasks"
        )

    def _generate_signals(self) -> List[Tuple[Signal, Dict]]:
        """
        Generate signals for all assets concurrently.
        """
        try:
            asset_list: List[Tuple[str, str]] = self.registry.all_assets()
            scan_state = self._classify_signal_candidates(asset_list)
            base_candidates = list(scan_state["base_candidates"])
            candidates = list(scan_state["tradable_candidates"])
            market_block_counts = cast(Counter[str], scan_state["market_block_counts"])
            self._log_signal_scan_overview(
                asset_total=len(asset_list),
                candidate_total=len(base_candidates),
                tradable_total=len(candidates),
                cooling_count=int(scan_state["cooling_count"]),
                open_position_count=int(scan_state["open_position_count"]),
                market_closed_count=sum(market_block_counts.values()),
            )

            if not candidates:
                self._log_empty_signal_scan(market_block_counts)
                return []

            if self._stop_event.is_set():
                return []

            result, status_counts, task_count = self._collect_generated_signals(candidates)
            status_counts["market_closed"] += sum(market_block_counts.values())
            self._log_signal_scan_summary(
                tradable_count=len(candidates),
                generated_count=len(result),
                status_counts=status_counts,
                market_block_counts=market_block_counts,
                task_count=task_count,
            )

        except Exception as e:
            logger.error(f"[TradingCore] Signal generation error: {e}")

        return result if "result" in locals() else []

    def _within_position_caps(self, signal: Signal) -> bool:
        from config.config import MAX_POSITIONS
        if self.state.open_position_count() >= MAX_POSITIONS:
            return False

        from config.config import CATEGORY_CAPS, CATEGORY_CAP_SOFT_BUFFER
        cat = signal.category
        cat_open = sum(
            1 for p in self.state.get_open_positions()
            if p.get("category") == cat
        )
        soft_cap = CATEGORY_CAPS.get(cat, 99)
        hard_cap = soft_cap + max(0, CATEGORY_CAP_SOFT_BUFFER)
        if cat_open >= hard_cap:
            logger.warning(
                f"[TradingCore] Category cap blocked {signal.asset}: "
                f"{cat_open} open {cat} positions >= hard cap {hard_cap}"
            )
            return False
        if cat_open >= soft_cap:
            logger.info(
                f"[TradingCore] Category soft cap exceeded for {signal.asset}: "
                f"{cat_open} open {cat} positions >= soft cap {soft_cap}; "
                f"allowing execution and deferring concentration control to portfolio risk"
            )
        return True

    def _passes_execution_risk_gate(self, signal: Signal) -> bool:
        # FIX S6: Call validate_signal so the daily loss guard is actually
        # enforced.  Previously this was never called → 5% daily loss halt
        # had no effect → bot could blow the account in a single bad day.
        if self._risk_manager:
            allowed, reason = self._risk_manager.validate_signal(
                confidence=signal.confidence,
                daily_pnl=self.state.daily_pnl,
                category=signal.category,
            )
            if not allowed:
                logger.warning(f"[TradingCore] Risk gate blocked {signal.asset}: {reason}")
                return False
        return True

    def _build_executable_signal_payload(self, signal: Signal) -> Dict[str, Any]:
        signal_dict = signal.to_dict()
        if self._risk_manager and float(signal_dict.get("position_size", 0) or 0) <= 0:
            metadata = signal_dict.get("metadata") if isinstance(signal_dict.get("metadata"), dict) else {}
            adaptive_policy = metadata.get("adaptive_policy") if isinstance(metadata.get("adaptive_policy"), dict) else {}
            risk_parameters = signal_dict.get("risk_parameters") if isinstance(signal_dict.get("risk_parameters"), dict) else {}
            adaptive_risk_multiplier = float(
                adaptive_policy.get("risk_multiplier")
                or risk_parameters.get("adaptive_risk_multiplier")
                or 1.0
            )
            try:
                signal_dict["position_size"] = self._risk_manager.calculate_position_size(
                    entry_price=float(signal_dict.get("entry_price", 0) or 0),
                    stop_loss=float(signal_dict.get("stop_loss", 0) or 0),
                    category=signal.category,
                    confidence=signal.confidence,
                    asset=signal.asset,
                    risk_multiplier=adaptive_risk_multiplier,
                )
            except Exception as _size_err:
                logger.debug(f"[TradingCore] Position sizing error for {signal.asset}: {_size_err}")
        addon_meta = (
            signal_dict.get("metadata", {}).get("winner_addon")
            if isinstance(signal_dict.get("metadata"), dict)
            else {}
        )
        if isinstance(addon_meta, dict) and addon_meta.get("enabled"):
            try:
                base_size = float(signal_dict.get("position_size", 0.0) or 0.0)
                size_fraction = max(0.15, min(0.70, float(addon_meta.get("size_fraction", 0.45) or 0.45)))
                if base_size > 0:
                    signal_dict["position_size"] = round(base_size * size_fraction, 6)
            except Exception as _addon_err:
                logger.debug(f"[TradingCore] Winner add-on sizing error for {signal.asset}: {_addon_err}")
        try:
            from risk.broker_sizer import BrokerPositionSizer

            signal_dict = BrokerPositionSizer.annotate_local(
                signal_dict,
                account_balance=self.state.balance,
            )
        except Exception as _broker_size_err:
            logger.debug(f"[TradingCore] Broker-neutral sizing metadata error for {signal.asset}: {_broker_size_err}")
        return signal_dict

    def _apply_universal_order_flow_gate(
        self,
        signal: Signal,
        signal_dict: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # Any asset carrying trusted depth can be blocked by hostile book
        # pressure, while provider-specific validators are optional add-ons.
        metadata = signal.metadata if isinstance(signal.metadata, dict) else {}
        micro = metadata.get("market_microstructure") if isinstance(metadata.get("market_microstructure"), dict) else {}
        direction_sign = 1 if str(signal.direction or "").upper() == "BUY" else -1 if str(signal.direction or "").upper() == "SELL" else 0
        try:
            depth_available = bool(micro.get("depth_available") or metadata.get("depth_available"))
            synthetic_depth = bool(micro.get("synthetic_depth_available") or metadata.get("synthetic_depth_available"))
            book = float(micro.get("book_imbalance", metadata.get("book_imbalance", 0.0)) or 0.0) * direction_sign
            flow = float(micro.get("score", metadata.get("microstructure_alignment", 0.0)) or 0.0) * direction_sign
            quality = float(micro.get("depth_quality", metadata.get("depth_quality", 0.0)) or 0.0)
            trust = float(micro.get("depth_provider_trust_score", metadata.get("depth_provider_trust_score", 0.0)) or 0.0)
            if depth_available and not synthetic_depth and max(book, flow) <= -0.24 and quality >= 0.20 and trust >= 0.42:
                logger.warning(f"[TradingCore] Depth gate blocked {signal.asset}: hostile book pressure")
                return None
        except Exception:
            pass

        try:
            from order_flow import get_validator

            validator = get_validator()
            allowed, reason = validator.validate_signal(signal_dict)
            if not allowed:
                logger.warning(f"[TradingCore] Order flow blocked {signal.asset}: {reason}")
                return None
            return validator.adjust_signal(signal_dict)
        except Exception as _ofe:
            logger.debug(f"[TradingCore] Order flow check error: {_ofe}")
            return signal_dict

    def _passes_portfolio_risk_gate(
        self,
        signal: Signal,
        signal_dict: Dict[str, Any],
    ) -> bool:
        # Portfolio risk must run on the final executable payload, after sizing
        # and order-flow adjustments, otherwise exposure checks see size=0.
        if self._portfolio_risk is not None:
            try:
                pr_allowed, pr_reason = self._portfolio_risk.evaluate(
                    signal=signal_dict,
                    open_positions=self.state.get_open_positions(),
                    balance=self.state.balance,
                    initial_balance=self.state.initial_balance,
                    daily_pnl=self.state.daily_pnl,
                )
                if not pr_allowed:
                    logger.warning(f"[TradingCore] PortfolioRisk blocked {signal.asset}: {pr_reason}")
                    return False
                if pr_reason:
                    logger.info(f"[TradingCore] PortfolioRisk resized {signal.asset}: {pr_reason}")
            except Exception as _pre:
                logger.debug(f"[TradingCore] PortfolioRisk check error: {_pre}")
        return True

    @staticmethod
    def _sync_signal_execution_fields(signal: Signal, signal_dict: Dict[str, Any]) -> None:
        try:
            signal.position_size = float(signal_dict.get("position_size", signal.position_size) or 0.0)
            signal.stop_loss = float(signal_dict.get("stop_loss", signal.stop_loss) or signal.stop_loss)
            signal.take_profit = float(signal_dict.get("take_profit", signal.take_profit) or signal.take_profit)
        except Exception:
            pass

    @staticmethod
    def _copy_signal_features(signal: Signal) -> None:
        if signal.metadata.get("features") is not None:
            try:
                signal.metadata["signal_features"] = list(signal.metadata["features"])
            except Exception:
                pass

    def _publish_open_trade(self, signal: Signal, trade: Dict[str, Any]) -> None:
        self.state.add_position(trade)
        logger.log_trade(
            "OPEN",
            asset=signal.asset,
            direction=signal.direction,
            score=f"{signal.confidence:.3f}",
            entry=signal.entry_price,
        )
        try:
            from redis_broker import broker as _redis_broker

            _redis_broker.publish_signal(signal.to_dict())
            _redis_broker.publish_positions(
                self.state.get_open_positions(),
                self.state.balance,
            )
        except Exception:
            pass

        self._notify_telegram_open(trade)

    def _ensure_router_paper_adapter(self, router: Any) -> None:
        if self._paper_trader is None or router is None:
            return
        try:
            has_adapter = getattr(router, "has_adapter", None)
            if callable(has_adapter) and has_adapter("paper"):
                return
            if not callable(has_adapter) and "paper" in getattr(router, "_adapters", {}):
                return
            from execution.paper_adapter import PaperAdapter

            router.register("paper", PaperAdapter(self._paper_trader))
        except Exception as exc:
            logger.debug(f"[TradingCore] Router paper adapter attach failed: {exc}")

    def _execute_signal(self, signal: Signal) -> bool:
        if not self._within_position_caps(signal):
            return False

        if not self._passes_execution_risk_gate(signal):
            return False

        signal_dict = self._build_executable_signal_payload(signal)
        signal_dict = self._apply_universal_order_flow_gate(signal, signal_dict) or {}
        if not signal_dict:
            return False

        if not self._passes_portfolio_risk_gate(signal, signal_dict):
            return False

        self._sync_signal_execution_fields(signal, signal_dict)

        if float(signal_dict.get("position_size", 0) or 0) <= 0:
            logger.warning(f"[TradingCore] Position size rejected for {signal.asset}")
            return False

        self._copy_signal_features(signal)

        try:
            router = getattr(self, "exchange_router", None)
            if router is not None:
                self._ensure_router_paper_adapter(router)
                result = router.submit(signal_dict)
                if result and result.status == "FILLED":
                    trade = result.raw.get("trade") if isinstance(result.raw, dict) else None
                    if not isinstance(trade, dict):
                        trade = result.raw if isinstance(result.raw, dict) else {}
                    if trade:
                        self._publish_open_trade(signal, trade)
                        return True
                    logger.error(f"[TradingCore] Router filled {signal.asset} without a trade payload")
                    return False
                if result:
                    logger.warning(f"[TradingCore] Router rejected {signal.asset}: {result.error or result.status}")
                    self._notify_broker_issue(
                        signal.asset,
                        signal.category,
                        str(result.error or result.status or ""),
                        stage="order submission",
                        action="order was not opened; signal remains rejected",
                    )
                return False

            trade = self._paper_trader.execute_signal(signal_dict)
            if trade:
                self._publish_open_trade(signal, trade)
                return True
        except Exception as e:
            logger.error(f"[TradingCore] Execute failed {signal.asset}: {e}")
        return False

    def _fetch_price_data(self, asset: str, category: str):
        if self.fetcher:
            try:
                timeframe = get_trading_timeframe(category)
                _periods = get_timeframe_periods(timeframe)
                return self.fetcher.get_ohlcv(
                    asset, category,
                    interval=timeframe,
                    periods=_periods,
                )
            except Exception:
                pass
        return None

    @staticmethod
    def _normalize_tp_size_fractions(raw_fractions: Any, total_tiers: int) -> List[float]:
        if total_tiers <= 0:
            return []
        parsed: List[float] = []
        for raw in list(raw_fractions or []):
            try:
                value = float(raw)
            except Exception:
                continue
            if value > 0:
                parsed.append(value)
        if not parsed:
            return [round(1.0 / total_tiers, 6) for _ in range(total_tiers)]
        parsed = parsed[:total_tiers]
        total = sum(parsed)
        if total <= 0:
            return [round(1.0 / total_tiers, 6) for _ in range(total_tiers)]
        return [round(value / total, 6) for value in parsed]

    @staticmethod
    def _build_take_profit_levels(
        entry: float,
        take_profit: float,
        direction: str,
        structure: Optional[Dict[str, Any]] = None,
    ) -> List[float]:
        levels: List[float] = []
        try:
            dist = abs(float(take_profit) - float(entry))
            if dist <= 0:
                return levels
            structure = structure if isinstance(structure, dict) else {}
            direction_key = str(direction).upper()
            target_key = "bullish_target_levels" if direction_key == "BUY" else "bearish_target_levels"
            structural_levels: List[float] = []
            for raw_level in list(structure.get(target_key) or []):
                try:
                    level = float(raw_level)
                except Exception:
                    continue
                if direction_key == "BUY" and level > entry:
                    structural_levels.append(round(level, 6))
                elif direction_key == "SELL" and level < entry:
                    structural_levels.append(round(level, 6))
            if structural_levels:
                levels = structural_levels[:3]
                if direction_key == "BUY" and take_profit > levels[-1] + max(dist * 0.05, 1e-6):
                    levels.append(round(float(take_profit), 6))
                elif direction_key == "SELL" and take_profit < levels[-1] - max(dist * 0.05, 1e-6):
                    levels.append(round(float(take_profit), 6))
                levels = list(dict.fromkeys(levels))[:4]
            elif direction_key == "BUY":
                levels = [round(entry + dist * 0.6, 6), round(entry + dist, 6), round(entry + dist * 1.25, 6)]
            else:
                levels = [round(entry - dist * 0.6, 6), round(entry - dist, 6), round(entry - dist * 1.25, 6)]
        except Exception:
            return []
        return levels

    def _load_execution_feedback_policy(
        self,
        *,
        asset: str,
        category: str,
        context: Dict[str, Any],
        policy_asset: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            from services.execution_feedback_service import get_service as get_execution_feedback_service

            return get_execution_feedback_service().get_exit_adjustment(
                policy_asset or asset,
                category,
                context,
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Execution feedback policy unavailable for {asset}: {exc}")
            return {}

    @staticmethod
    def _execution_feedback_multipliers(execution_feedback_policy: Dict[str, Any]) -> Tuple[float, float]:
        stop_buffer_multiplier = float(
            execution_feedback_policy.get("stop_buffer_multiplier", 1.0) or 1.0
        )
        target_rr_multiplier = float(
            execution_feedback_policy.get("target_rr_multiplier", 1.0) or 1.0
        )
        return stop_buffer_multiplier, target_rr_multiplier

    def _resolve_stop_loss(
        self,
        *,
        entry_price: float,
        direction: str,
        category: str,
        atr: float,
        stop_buffer_multiplier: float,
        structure: Optional[Dict[str, Any]] = None,
    ) -> float:
        if self._risk_manager is not None:
            scaled_stop_fn = getattr(self._risk_manager, "get_stop_loss_scaled", None)
            stop_loss = None
            if callable(scaled_stop_fn):
                try:
                    scaled_stop = self._call_optional_stop_scaler(
                        cast(Callable[..., Any], scaled_stop_fn),
                        entry_price,
                        direction,
                        category,
                        atr=atr,
                        distance_multiplier=stop_buffer_multiplier,
                        structure=structure if isinstance(structure, dict) else {},
                    )
                    if isinstance(scaled_stop, (int, float)):
                        stop_loss = float(scaled_stop)
                except Exception:
                    stop_loss = None
            if stop_loss is None:
                stop_loss = self._risk_manager.get_stop_loss(
                    entry_price,
                    direction,
                    category,
                    atr=atr,
                )
            return float(stop_loss)

        distance = (atr * 1.5 if atr > 0 else entry_price * 0.006) * max(
            0.75,
            min(1.25, stop_buffer_multiplier),
        )
        if direction == "BUY":
            return float(entry_price - distance)
        return float(entry_price + distance)

    @staticmethod
    def _tighten_stop_loss(
        *,
        current_stop_loss: float,
        proposed_stop_loss: float,
        direction: str,
        tighten_only: bool,
    ) -> float:
        if not (tighten_only and current_stop_loss > 0):
            return float(proposed_stop_loss)
        if direction == "BUY":
            return float(max(current_stop_loss, proposed_stop_loss))
        return float(min(current_stop_loss, proposed_stop_loss))

    def _resolve_take_profit(
        self,
        *,
        entry_price: float,
        stop_loss: float,
        direction: str,
        category: str,
        atr: float,
        target_rr_multiplier: float,
    ) -> float:
        if self._risk_manager is not None:
            try:
                take_profit = self._risk_manager.get_take_profit(
                    entry_price,
                    stop_loss,
                    direction,
                    category=category,
                    rr_multiplier=target_rr_multiplier,
                )
            except TypeError:
                take_profit = self._risk_manager.get_take_profit(
                    entry_price,
                    stop_loss,
                    direction,
                    category=category,
                )
            return float(take_profit)

        distance = abs(entry_price - stop_loss)
        reward_distance = distance * 1.8 * max(0.82, min(1.24, target_rr_multiplier))
        if direction == "BUY":
            return float(entry_price + reward_distance)
        return float(entry_price - reward_distance)

    @staticmethod
    def _structure_target_alignment_payload(
        *,
        base_take_profit: float,
        aligned_take_profit: float,
        structure: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "applied": True,
            "base_take_profit": round(float(base_take_profit), 6),
            "aligned_take_profit": round(float(aligned_take_profit), 6),
            "regime": str((structure or {}).get("regime") or ""),
            "structure_bias": str((structure or {}).get("structure_bias") or ""),
        }

    def _align_take_profit_to_structure(
        self,
        *,
        asset: str,
        entry_price: float,
        take_profit: float,
        direction: str,
        category: str,
        structure: Dict[str, Any],
        atr: float,
        confidence: float,
    ) -> Tuple[float, Dict[str, Any]]:
        if self._risk_manager is None:
            return float(take_profit), {}
        align_tp_fn = getattr(self._risk_manager, "align_take_profit_to_structure", None)
        if not callable(align_tp_fn):
            return float(take_profit), {}
        try:
            aligned_take_profit = self._call_optional_tp_aligner(
                cast(Callable[..., Any], align_tp_fn),
                entry_price,
                take_profit,
                direction,
                category=category,
                structure=structure if isinstance(structure, dict) else {},
                atr=atr,
                confidence=confidence,
            )
            if not isinstance(aligned_take_profit, (int, float)) or aligned_take_profit <= 0:
                return float(take_profit), {}
            aligned_take_profit = float(aligned_take_profit)
            if abs(aligned_take_profit - float(take_profit)) <= 1e-9:
                return float(aligned_take_profit), {}
            return aligned_take_profit, self._structure_target_alignment_payload(
                base_take_profit=float(take_profit),
                aligned_take_profit=aligned_take_profit,
                structure=structure if isinstance(structure, dict) else {},
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Structure target alignment unavailable for {asset}: {exc}")
            return float(take_profit), {}

    @staticmethod
    def _build_reprice_metadata(
        *,
        snapshot: Dict[str, Any],
        atr: float,
        execution_feedback_policy: Dict[str, Any],
        target_rr_multiplier: float,
        stop_buffer_multiplier: float,
        structure_target_alignment: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            **dict(snapshot.get("metadata") or {}),
            "atr": round(atr, 6) if atr > 0 else 0.0,
            "exit_model": "atr" if atr > 0 else "category_fallback",
            "repriced_at_utc": datetime.utcnow().isoformat(),
            "execution_feedback_policy": execution_feedback_policy,
            "execution_quality_score": round(
                float(execution_feedback_policy.get("avg_quality_score", 50.0) or 50.0),
                1,
            ),
            "execution_feedback_sample_count": int(execution_feedback_policy.get("sample_count", 0) or 0),
            "target_rr_multiplier": round(target_rr_multiplier, 4),
            "stop_buffer_multiplier": round(stop_buffer_multiplier, 4),
            "structure_target_alignment": structure_target_alignment,
        }

    @staticmethod
    def _build_reprice_update(
        *,
        trade_id: str,
        asset: str,
        category: str,
        direction: str,
        entry_price: float,
        current_stop_loss: float,
        current_take_profit: float,
        snapshot: Dict[str, Any],
        atr: float,
        execution_feedback_policy: Dict[str, Any],
        target_rr_multiplier: float,
        stop_buffer_multiplier: float,
    ) -> Dict[str, Any]:
        return {
            "trade_id": trade_id,
            "asset": asset,
            "category": category,
            "direction": direction,
            "entry_price": entry_price,
            "old_stop_loss": current_stop_loss,
            "new_stop_loss": snapshot["stop_loss"],
            "old_take_profit": current_take_profit,
            "new_take_profit": snapshot["take_profit"],
            "atr": round(atr, 6) if atr > 0 else 0.0,
            "execution_quality_score": round(
                float(execution_feedback_policy.get("avg_quality_score", 50.0) or 50.0),
                1,
            ),
            "target_rr_multiplier": round(target_rr_multiplier, 4),
            "stop_buffer_multiplier": round(stop_buffer_multiplier, 4),
        }

    @staticmethod
    def _build_playbook_trade_management_plan(
        *,
        direction: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        structure: Optional[Dict[str, Any]],
        playbook_direction: str,
        playbook_action: str,
        playbook_management_template: Dict[str, Any],
        playbook_interval: str,
        playbook_entry_style: str,
        playbook_pick: Dict[str, Any],
        playbook_primary: Dict[str, Any],
    ) -> Tuple[float, List[float], Dict[str, Any]]:
        trade_management_plan: Dict[str, Any] = {}
        structure = structure if isinstance(structure, dict) else {}
        take_profit_levels = TradingCore._build_take_profit_levels(
            entry_price,
            take_profit,
            direction,
            structure=structure,
        )
        risk_distance = abs(entry_price - stop_loss)
        if not (
            playbook_direction == direction
            and playbook_action in {"seed", "override", "support"}
            and risk_distance > 0.0
            and playbook_management_template
        ):
            return take_profit, take_profit_levels, trade_management_plan

        partial_rrs: List[float] = []
        for raw_rr in list(playbook_management_template.get("partial_take_profit_rr") or []):
            try:
                rr_value = float(raw_rr)
            except Exception:
                continue
            if rr_value > 0.0:
                partial_rrs.append(rr_value)
        partial_size_fractions = TradingCore._normalize_tp_size_fractions(
            playbook_management_template.get("partial_take_profit_size_fractions"),
            len(partial_rrs) + 1,
        )

        runner_target_rr = float(playbook_management_template.get("runner_target_rr", 0.0) or 0.0)
        current_rr = abs(float(take_profit) - float(entry_price)) / max(risk_distance, 1e-9)
        if runner_target_rr > 0.0:
            runner_target_rr = max(runner_target_rr, current_rr)
            reward_distance = risk_distance * runner_target_rr
            runner_target = entry_price + reward_distance if direction == "BUY" else entry_price - reward_distance
            if direction == "BUY":
                take_profit = max(float(take_profit), float(runner_target))
            else:
                take_profit = min(float(take_profit), float(runner_target))
            if take_profit_levels:
                if direction == "BUY" and take_profit > float(take_profit_levels[-1]):
                    take_profit_levels.append(round(float(take_profit), 6))
                elif direction == "SELL" and take_profit < float(take_profit_levels[-1]):
                    take_profit_levels.append(round(float(take_profit), 6))
            else:
                level_rrs = sorted({round(rr, 4) for rr in partial_rrs + [runner_target_rr] if rr > 0.0})
                for level_rr in level_rrs:
                    reward = risk_distance * level_rr
                    level_price = entry_price + reward if direction == "BUY" else entry_price - reward
                    take_profit_levels.append(round(level_price, 6))

        trade_management_plan = {
            **playbook_management_template,
            "partial_take_profit_rr": [round(float(rr), 4) for rr in partial_rrs],
            "partial_take_profit_size_fractions": partial_size_fractions,
            "runner_target_rr": round(runner_target_rr or current_rr, 4),
            "preferred_interval": playbook_interval or playbook_management_template.get("preferred_interval") or "",
            "entry_style": playbook_entry_style,
            "session": str(playbook_pick.get("session") or playbook_primary.get("session") or ""),
        }
        return take_profit, take_profit_levels, trade_management_plan

    def _resolve_playbook_seed(
        self,
        asset: str,
        canonical: str,
        category: str,
        price_data,
        context: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], str, Any]:
        playbook_pick: Dict[str, Any] = {"action": "", "primary": None, "candidates": []}
        playbook_interval = ""
        playbook_price_data = price_data
        try:
            from services.playbook_service import get_service as get_playbook_service

            playbook_service = get_playbook_service()
            playbook_interval = str(playbook_service.preferred_interval(category, canonical) or "").strip().lower()
            current_interval = str(context.get("timeframe") or get_trading_timeframe(category) or "").strip().lower()
            predictor = self._extract_predictor_runtime(context)
            fetcher = context.get("fetcher") or getattr(self, "fetcher", None)
            if (
                fetcher is not None
                and playbook_interval
                and playbook_interval != current_interval
            ):
                try:
                    fetched_frame = fetcher.get_ohlcv(
                        canonical,
                        category,
                        interval=playbook_interval,
                        periods=get_timeframe_periods(playbook_interval),
                    )
                    if fetched_frame is not None and not getattr(fetched_frame, "empty", True):
                        playbook_price_data = fetched_frame
                except Exception as exc:
                    logger.debug(f"[TradingCore] Playbook timeframe fetch unavailable for {asset}: {exc}")
            playbook_pick = (
                playbook_service.pick_seed(
                    canonical,
                    category,
                    playbook_price_data,
                    context,
                    ml_direction="BUY" if predictor["available"] and float(predictor["prediction"]) > 0.5 else "SELL" if predictor["available"] else "",
                    ml_confidence=float(predictor["confidence"] or 0.0),
                )
                or playbook_pick
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Playbook seed unavailable for {asset}: {exc}")
        return playbook_pick, playbook_interval, playbook_price_data

    def _build_seed_exit_plan(
        self,
        *,
        asset: str,
        canonical: str,
        category: str,
        direction: str,
        entry_price: float,
        price_data,
        context: Dict[str, Any],
        structure: Dict[str, Any],
        seed_confidence: float,
        playbook_direction: str,
        playbook_action: str,
        playbook_management_template: Dict[str, Any],
        playbook_interval: str,
        playbook_entry_style: str,
        playbook_pick: Dict[str, Any],
        playbook_primary: Dict[str, Any],
    ) -> Dict[str, Any]:
        atr = self._estimate_atr(price_data)
        execution_feedback_policy = self._load_execution_feedback_policy(
            asset=asset,
            policy_asset=canonical,
            category=category,
            context=context,
        )
        stop_buffer_multiplier, target_rr_multiplier = self._execution_feedback_multipliers(
            execution_feedback_policy
        )
        stop_loss = self._resolve_stop_loss(
            entry_price=entry_price,
            direction=direction,
            category=category,
            atr=atr,
            stop_buffer_multiplier=stop_buffer_multiplier,
            structure=structure if isinstance(structure, dict) else {},
        )
        take_profit = self._resolve_take_profit(
            entry_price=entry_price,
            stop_loss=stop_loss,
            direction=direction,
            category=category,
            atr=atr,
            target_rr_multiplier=target_rr_multiplier,
        )
        take_profit, structure_target_alignment = self._align_take_profit_to_structure(
            asset=asset,
            entry_price=entry_price,
            take_profit=take_profit,
            direction=direction,
            category=category,
            structure=structure if isinstance(structure, dict) else {},
            atr=atr,
            confidence=seed_confidence,
        )
        take_profit, take_profit_levels, trade_management_plan = self._build_playbook_trade_management_plan(
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            structure=structure if isinstance(structure, dict) else {},
            playbook_direction=playbook_direction,
            playbook_action=playbook_action,
            playbook_management_template=playbook_management_template,
            playbook_interval=playbook_interval,
            playbook_entry_style=playbook_entry_style,
            playbook_pick=playbook_pick,
            playbook_primary=playbook_primary,
        )
        return {
            "atr": atr,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "take_profit_levels": take_profit_levels,
            "trade_management_plan": trade_management_plan,
            "execution_feedback_policy": execution_feedback_policy,
            "target_rr_multiplier": target_rr_multiplier,
            "stop_buffer_multiplier": stop_buffer_multiplier,
            "structure_target_alignment": structure_target_alignment,
        }

    @staticmethod
    def _fmt_metric(value: Any, digits: int = 3) -> str:
        try:
            if value is None:
                return "n/a"
            return f"{float(value):.{digits}f}"
        except Exception:
            return "n/a"

    @classmethod
    def _fmt_predictor_pair(cls, prediction: Any, confidence: Any) -> str:
        try:
            conf = float(confidence)
        except Exception:
            conf = 0.0
        if prediction is None or conf <= 0.10:
            return "n/a"
        return f"{cls._fmt_metric(prediction)}/{cls._fmt_metric(confidence)}"

    @staticmethod
    def _extract_predictor_runtime(context: Dict[str, Any]) -> Dict[str, Any]:
        prediction = context.get("predictor_prediction")
        if prediction is None:
            prediction = context.get("ml_prediction")
        try:
            confidence = float(
                context.get(
                    "predictor_confidence",
                    context.get("ml_confidence", 0.0),
                ) or 0.0
            )
        except Exception:
            confidence = 0.0
        model = str(
            context.get("predictor_model")
            or context.get("ml_model")
            or ""
        ).strip()
        provider = str(
            context.get("predictor_provider")
            or context.get("ml_provider")
            or ""
        ).strip()
        available = bool(prediction is not None and confidence > 0.10)
        return {
            "prediction": prediction if available else None,
            "confidence": confidence if available else 0.0,
            "model": model,
            "provider": provider,
            "available": available,
        }

    @staticmethod
    def _fmt_reason_list(value: Any, limit: int = 3) -> str:
        if isinstance(value, (list, tuple)):
            items = [str(item).strip() for item in value if str(item).strip()]
        elif value:
            items = [str(value).strip()]
        else:
            items = []
        if not items:
            return "n/a"
        return ",".join(items[: max(1, int(limit or 1))])

    @staticmethod
    def _fmt_reason_buckets(value: Any, limit: int = 4) -> str:
        if isinstance(value, (list, tuple)):
            items = [str(item).strip() for item in value if str(item).strip()]
        elif value:
            items = [str(value).strip()]
        else:
            items = []
        if not items:
            return "n/a"
        buckets = Counter()
        for item in items:
            bucket = item.split(":", 1)[0].strip().lower() if ":" in item else item.strip().lower()
            if bucket:
                buckets[bucket] += 1
        if not buckets:
            return "n/a"
        return "|".join(
            f"{label}={count}"
            for label, count in buckets.most_common(max(1, int(limit or 1)))
        )

    @staticmethod
    def _call_optional_stop_scaler(
        scaled_stop_fn: Callable[..., Any],
        entry: float,
        direction: str,
        category: str,
        *,
        atr: float,
        distance_multiplier: float,
        structure: Dict[str, Any],
    ) -> Any:
        return scaled_stop_fn(
            entry,
            direction,
            category,
            atr=atr,
            distance_multiplier=distance_multiplier,
            structure=structure,
        )

    @staticmethod
    def _call_optional_tp_aligner(
        align_tp_fn: Callable[..., Any],
        entry: float,
        take_profit: float,
        direction: str,
        *,
        category: str,
        structure: Dict[str, Any],
        atr: float,
        confidence: float,
    ) -> Any:
        return align_tp_fn(
            entry,
            take_profit,
            direction,
            category=category,
            structure=structure,
            atr=atr,
            confidence=confidence,
        )

    @staticmethod
    def _command_center_context_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value if value not in (None, "") else default)
        except Exception:
            return default

    @staticmethod
    def _command_center_context_int(value: Any, default: int = 0) -> int:
        try:
            return int(value if value not in (None, "") else default)
        except Exception:
            return default

    @staticmethod
    def _command_center_context_direction(*values: Any) -> str:
        for value in values:
            token = str(value or "").strip().lower()
            if token in {"buy", "sell"}:
                return token.upper()
        return ""

    def _build_command_center_context_row(
        self,
        asset: str,
        context: Dict[str, Any],
        reason: str,
    ) -> Dict[str, Any]:
        seed_decision = dict(context.get("seed_decision") or {})
        playbook_decision = dict(context.get("playbook_decision") or {})
        signal_metadata = dict(context.get("signal_metadata") or {})
        structure = dict(context.get("market_structure") or {})
        adaptive_policy_raw = context.get("adaptive_policy")
        if isinstance(adaptive_policy_raw, dict):
            adaptive_policy = dict(
                adaptive_policy_raw.get("raw")
                if isinstance(adaptive_policy_raw.get("raw"), dict)
                else adaptive_policy_raw
            )
        else:
            adaptive_policy = {}
        broker_quality = dict(
            context.get("broker_quality")
            if isinstance(context.get("broker_quality"), dict)
            else signal_metadata.get("broker_quality")
            if isinstance(signal_metadata.get("broker_quality"), dict)
            else {}
        )
        market_microstructure = dict(
            context.get("market_microstructure")
            if isinstance(context.get("market_microstructure"), dict)
            else signal_metadata.get("market_microstructure")
            if isinstance(signal_metadata.get("market_microstructure"), dict)
            else {}
        )
        cross_asset_context = dict(
            context.get("cross_asset_context")
            if isinstance(context.get("cross_asset_context"), dict)
            else signal_metadata.get("cross_asset_context")
            if isinstance(signal_metadata.get("cross_asset_context"), dict)
            else {}
        )

        current_interval = str(context.get("timeframe") or "").strip().lower() or "n/a"
        playbook_interval = (
            str(
                playbook_decision.get("preferred_interval")
                or seed_decision.get("playbook_timeframe")
                or signal_metadata.get("playbook_timeframe")
                or ""
            ).strip().lower()
            or current_interval
        )
        session_label = (
            str(
                playbook_decision.get("session_label")
                or playbook_decision.get("session")
                or seed_decision.get("session")
                or signal_metadata.get("session_label")
                or signal_metadata.get("session")
                or ""
            ).strip().lower()
            or "n/a"
        )
        structure_bias = str(
            structure.get("structure_bias")
            or seed_decision.get("structure_bias")
            or signal_metadata.get("structure_bias")
            or "neutral"
        ).strip().lower()
        direction = self._command_center_context_direction(
            playbook_decision.get("direction"),
            seed_decision.get("direction"),
            signal_metadata.get("playbook_direction"),
            signal_metadata.get("direction"),
            structure_bias,
        )
        alignment_score = round(
            self._command_center_context_float(
                structure.get("alignment_score", seed_decision.get("alignment_score")),
                0.0,
            ),
            4,
        )
        setup_quality = round(
            self._command_center_context_float(
                structure.get("setup_quality", seed_decision.get("setup_quality")),
                0.0,
            ),
            4,
        )
        context_confluence = round(
            self._command_center_context_float(
                playbook_decision.get("context_confluence", signal_metadata.get("playbook_context_confluence")),
                0.0,
            ),
            4,
        )
        cross_alignment = round(
            self._command_center_context_float(
                playbook_decision.get("cross_alignment", signal_metadata.get("playbook_cross_alignment")),
                0.0,
            ),
            4,
        )
        micro_score = round(
            self._command_center_context_float(
                playbook_decision.get("micro_score", signal_metadata.get("playbook_micro_score")),
                0.0,
            ),
            4,
        )
        whale_context_support = round(
            self._command_center_context_float(
                playbook_decision.get("whale_context_support", signal_metadata.get("playbook_whale_context_support")),
                0.0,
            ),
            4,
        )
        support_components = self._command_center_context_int(
            playbook_decision.get("support_components", signal_metadata.get("playbook_support_components", 0)),
            0,
        )
        conflict_components = self._command_center_context_int(
            playbook_decision.get("conflict_components", signal_metadata.get("playbook_conflict_components", 0)),
            0,
        )
        candidate_count = self._command_center_context_int(
            playbook_decision.get("candidate_count", seed_decision.get("candidate_count", 0)),
            0,
        )
        blocked_reason = str(
            playbook_decision.get("blocked_reason")
            or seed_decision.get("blocked_reason")
            or signal_metadata.get("blocked_reason")
            or ""
        ).strip()
        if blocked_reason.lower() == "n/a":
            blocked_reason = ""
        rejected_reasons = list(
            playbook_decision.get("rejected_reasons")
            or seed_decision.get("rejected_reasons")
            or signal_metadata.get("rejected_reasons")
            or []
        )
        rejected_details = list(
            playbook_decision.get("rejected_details")
            or seed_decision.get("rejected_details")
            or signal_metadata.get("rejected_details")
            or []
        )
        decision_reason = str(reason or blocked_reason or "no_playbook_seed").strip() or "no_playbook_seed"
        if decision_reason == "no_playbook_seed" and not blocked_reason and candidate_count <= 0:
            decision_reason = "waiting_for_playbook_seed"
            decision_state = "Watching"
            decision_kind = "watching_seed"
        else:
            decision_state = "Blocked"
            decision_kind = "preseed_blocked"
        derived_opportunity = max(
            0.0,
            min(
                1.0,
                (
                    setup_quality * 0.34
                    + alignment_score * 0.24
                    + context_confluence * 0.16
                    + max(0.0, cross_alignment) * 0.08
                    + micro_score * 0.10
                    + whale_context_support * 0.04
                    + min(3, support_components) * 0.02
                    - min(3, conflict_components) * 0.03
                ),
            ),
        )
        confidence = self._command_center_context_float(
            playbook_decision.get(
                "confidence",
                signal_metadata.get(
                    "confidence",
                    signal_metadata.get(
                        "playbook_confidence",
                        playbook_decision.get("score", derived_opportunity),
                    ),
                ),
            ),
            0.0,
        )
        opportunity_score = self._command_center_context_float(
            signal_metadata.get(
                "opportunity_score",
                playbook_decision.get("score", derived_opportunity),
            ),
            0.0,
        )
        now_ts = int(time.time() * 1000)

        metadata = {
            **signal_metadata,
            "playbook_name": str(
                signal_metadata.get("playbook_name")
                or playbook_decision.get("playbook")
                or seed_decision.get("playbook")
                or ""
            ),
            "playbook_entry_style": str(
                signal_metadata.get("playbook_entry_style")
                or playbook_decision.get("entry_style")
                or playbook_decision.get("playbook_entry_style")
                or "n/a"
            ).strip().lower()
            or "n/a",
            "entry_style": str(
                signal_metadata.get("entry_style")
                or playbook_decision.get("entry_style")
                or playbook_decision.get("playbook_entry_style")
                or "n/a"
            ).strip().lower()
            or "n/a",
            "playbook_timeframe": playbook_interval,
            "session_label": session_label,
            "session": session_label,
            "pattern_family": str(structure.get("pattern_family") or signal_metadata.get("pattern_family") or ""),
            "structure_bias": structure_bias,
            "alignment_score": alignment_score,
            "setup_quality": setup_quality,
            "entry_confirmation_ready": bool(structure.get("entry_confirmation_ready")),
            "entry_confirmation_count": self._command_center_context_int(structure.get("entry_confirmation_count", 0), 0),
            "entry_confirmation_bars_required": self._command_center_context_int(
                structure.get("entry_confirmation_bars_required", 0),
                0,
            ),
            "breakout_retest_ready": bool(structure.get("breakout_retest_ready")),
            "first_pullback_ready": bool(structure.get("first_pullback_ready")),
            "failed_opposite_move_confirmed": bool(structure.get("failed_opposite_move_confirmed")),
            "liquidity_sweep_reclaim": bool(structure.get("liquidity_sweep_reclaim")),
            "elite_pattern_rank": round(self._command_center_context_float(structure.get("elite_pattern_rank"), 0.0), 4),
            "cluster_penalty": round(self._command_center_context_float(structure.get("cluster_penalty"), 0.0), 4),
            "impulse_age_bars": self._command_center_context_int(structure.get("impulse_age_bars", 0), 0),
            "extension_score": round(self._command_center_context_float(structure.get("extension_score"), 0.0), 4),
            "candle_quality_score": round(
                self._command_center_context_float(
                    structure.get("candle_quality_score", structure.get("trigger_candle_quality", 0.0)),
                    0.0,
                ),
                4,
            ),
            "session_quality_score": round(self._command_center_context_float(structure.get("session_quality_score"), 0.0), 4),
            "target_efficiency_score": round(self._command_center_context_float(structure.get("target_efficiency_score"), 0.0), 4),
            "blocked_reason": blocked_reason,
            "rejected_reasons": rejected_reasons,
            "rejected_details": rejected_details,
            "execution_kill_reason": decision_reason if decision_state == "Blocked" else "",
            "exact_kill_reason": decision_reason if decision_state == "Blocked" else "",
            "opportunity_score": round(opportunity_score, 4),
            "memory_score": round(self._command_center_context_float(signal_metadata.get("memory_score"), 0.0), 1),
            "memory_sample_count": self._command_center_context_int(signal_metadata.get("memory_sample_count", 0), 0),
            "execution_quality_score": round(
                self._command_center_context_float(signal_metadata.get("execution_quality_score"), 0.0),
                1,
            ),
            "execution_feedback_sample_count": self._command_center_context_int(
                signal_metadata.get("execution_feedback_sample_count", 0),
                0,
            ),
            "broker_quality": broker_quality,
            "market_microstructure": market_microstructure,
            "cross_asset_context": cross_asset_context,
            "adaptive_policy": adaptive_policy,
            "context_confluence": context_confluence,
            "cross_alignment": cross_alignment,
            "micro_score": micro_score,
            "whale_context_support": whale_context_support,
            "support_components": support_components,
            "conflict_components": conflict_components,
            "sentiment_score": round(
                self._command_center_context_float(
                    context.get("sentiment_score", signal_metadata.get("sentiment_score", 0.0)),
                    0.0,
                ),
                4,
            ),
            "market_structure": structure,
        }

        row = {
            "type": "command_center_context",
            "asset": asset,
            "category": str(context.get("category") or signal_metadata.get("category") or ""),
            "direction": direction,
            "signal": direction,
            "confidence": round(confidence, 4),
            "decision_kind": decision_kind,
            "decision_state": decision_state,
            "decision_reason": decision_reason,
            "reason": decision_reason,
            "blocked_reason": blocked_reason,
            "current_session": session_label,
            "timeframe": current_interval,
            "playbook_timeframe": playbook_interval,
            "candidate_count": candidate_count,
            "decision": decision_state.upper(),
            "metadata": metadata,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ts": now_ts,
        }
        row.update(
            {
                "opportunity_score": metadata["opportunity_score"],
                "memory_score": metadata["memory_score"],
                "execution_quality_score": metadata["execution_quality_score"],
                "alignment_score": metadata["alignment_score"],
                "setup_quality": metadata["setup_quality"],
                "entry_confirmation_ready": metadata["entry_confirmation_ready"],
                "entry_confirmation_count": metadata["entry_confirmation_count"],
                "entry_confirmation_bars_required": metadata["entry_confirmation_bars_required"],
                "breakout_retest_ready": metadata["breakout_retest_ready"],
                "first_pullback_ready": metadata["first_pullback_ready"],
                "failed_opposite_move_confirmed": metadata["failed_opposite_move_confirmed"],
                "elite_pattern_rank": metadata["elite_pattern_rank"],
                "cluster_penalty": metadata["cluster_penalty"],
                "extension_score": metadata["extension_score"],
                "candle_quality_score": metadata["candle_quality_score"],
                "session_quality_score": metadata["session_quality_score"],
                "target_efficiency_score": metadata["target_efficiency_score"],
                "broker_quality": broker_quality,
                "market_microstructure": market_microstructure,
                "cross_asset_context": cross_asset_context,
                "adaptive_policy": adaptive_policy,
            }
        )
        return row

    def _publish_command_center_context(self, asset: str, context: Dict[str, Any], reason: str) -> None:
        try:
            from services.redis_pool import get_client as _get_redis_client

            redis_client = _get_redis_client()
            if redis_client is None:
                return
            payload = self._build_command_center_context_row(asset, context, reason)
            raw = json.dumps(payload, default=str)
            redis_client.publish(COMMAND_CENTER_CONTEXT_UPDATE_CHANNEL, raw)
            redis_client.lpush(COMMAND_CENTER_CONTEXT_LOG_KEY, raw)
            redis_client.ltrim(COMMAND_CENTER_CONTEXT_LOG_KEY, 0, COMMAND_CENTER_CONTEXT_LOG_LIMIT)
        except Exception as exc:
            logger.debug(f"[TradingCore] command-center context publish failed: {exc}")

    def _log_seed_decision(self, asset: str, context: Dict[str, Any], reason: str) -> None:
        if context.get("_inspection_only"):
            return
        seed_decision = dict(context.get("seed_decision") or {})
        playbook_decision = dict(context.get("playbook_decision") or {})
        inactivity_profile = dict(playbook_decision.get("inactivity_profile") or _seed_inactivity_profile(context))
        structure = dict(context.get("market_structure") or {})
        current_interval = str(context.get("timeframe") or "").strip().lower() or "n/a"
        playbook_interval = (
            str(playbook_decision.get("preferred_interval") or seed_decision.get("playbook_timeframe") or "").strip().lower()
            or current_interval
        )
        session_label = (
            str(playbook_decision.get("session_label") or playbook_decision.get("session") or seed_decision.get("session") or "").strip().lower()
            or "n/a"
        )
        rejected_reasons = playbook_decision.get("rejected_reasons") or seed_decision.get("rejected_reasons") or []
        rejected_details = playbook_decision.get("rejected_details") or seed_decision.get("rejected_details") or []
        candidate_count = playbook_decision.get("candidate_count", seed_decision.get("candidate_count", 0))
        blocked_reason = str(playbook_decision.get("blocked_reason") or seed_decision.get("blocked_reason") or "").strip() or "n/a"
        entry_style = str(playbook_decision.get("entry_style") or "n/a").strip().lower() or "n/a"
        generic_flow_override = (
            str(playbook_decision.get("generic_flow_override_source") or "").strip().lower()
            if bool(playbook_decision.get("generic_flow_override"))
            else "off"
        )
        confirmation_count = int(structure.get("entry_confirmation_count", 0) or 0)
        confirmation_required = int(structure.get("entry_confirmation_bars_required", 0) or 0)
        logger.info(
            f"[TradingCore] Decision {asset} no_seed "
            f"reason={reason} "
            f"blocked={blocked_reason} "
            f"session={session_label} "
            f"tf={current_interval}->{playbook_interval} "
            f"bias={str(structure.get('structure_bias', seed_decision.get('structure_bias', 'neutral'))).lower()} "
            f"entry_style={entry_style} "
            f"generic_flow_override={generic_flow_override} "
            f"align={self._fmt_metric(structure.get('alignment_score', seed_decision.get('alignment_score')))} "
            f"setup={self._fmt_metric(structure.get('setup_quality', seed_decision.get('setup_quality')))} "
            f"candidates={candidate_count} "
            f"rejected={self._fmt_reason_list(rejected_reasons)} "
            f"reject_buckets={self._fmt_reason_buckets(rejected_reasons)} "
            f"reject_details={self._fmt_reason_list(rejected_details, limit=2)} "
            f"family={str(structure.get('pattern_family', 'unknown') or 'unknown').lower()} "
            f"trend5m={str(structure.get('trend_5m', 'unknown') or 'unknown').lower()} "
            f"trigger_align={int(bool(structure.get('trigger_trend_aligned')))} "
            f"confirm={confirmation_count}/{confirmation_required} "
            f"confirm_ready={int(bool(structure.get('entry_confirmation_ready')))} "
            f"fast_confirm={int(bool(structure.get('fast_entry_confirmation_ready')))} "
            f"retest={int(bool(structure.get('breakout_retest_ready')))} "
            f"pullback={int(bool(structure.get('first_pullback_ready')))} "
            f"reclaim={int(bool(structure.get('failed_opposite_move_confirmed')))} "
            f"ext={self._fmt_metric(structure.get('extension_score'))} "
            f"tgt={self._fmt_metric(structure.get('target_efficiency_score'))} "
            f"rank={self._fmt_metric(structure.get('elite_pattern_rank'))} "
            f"cluster={self._fmt_metric(structure.get('cluster_penalty'))} "
            f"inactivity={int(bool(inactivity_profile.get('active')))} "
            f"flat_book={int(bool(inactivity_profile.get('flat_book')))} "
            f"equity={int(bool(inactivity_profile.get('equity_relief')))} "
            f"relief={self._fmt_metric(inactivity_profile.get('relief_strength'))} "
            f"cat_recent={self._fmt_metric(inactivity_profile.get('category_recent_count'))} "
            f"asset_recent={self._fmt_metric(inactivity_profile.get('asset_recent_count'))} "
            f"ctx={self._fmt_metric(playbook_decision.get('context_confluence'))} "
            f"cross={self._fmt_metric(playbook_decision.get('cross_alignment'))} "
            f"micro={self._fmt_metric(playbook_decision.get('micro_score'))} "
            f"whale={self._fmt_metric(playbook_decision.get('whale_context_support'))} "
            f"shock={self._fmt_metric(playbook_decision.get('shock_score'))} "
            f"shock_evt={self._fmt_metric(playbook_decision.get('shock_event_score'))} "
            f"shock_liq={self._fmt_metric(playbook_decision.get('shock_liquidity_score'))} "
            f"shock_ok={int(bool(playbook_decision.get('shock_supported')))} "
            f"support={int(playbook_decision.get('support_components', 0) or 0)} "
            f"conflict={int(playbook_decision.get('conflict_components', 0) or 0)} "
            f"predictor={self._fmt_predictor_pair(context.get('predictor_prediction', context.get('ml_prediction')), context.get('predictor_confidence', context.get('ml_confidence')))} "
            f"sent={self._fmt_metric(context.get('sentiment_score'))} "
            f"funding={context.get('funding_bias', 'NEUTRAL')} "
            f"oi={context.get('oi_signal', 'NEUTRAL')}"
        )
        self._publish_command_center_context(asset, context, reason)

    def _log_decision_rejection(self, signal: Signal, context: Dict[str, Any]) -> None:
        reason = signal.kill_reason or signal.metadata.get("agent_rejection_reason", "killed")
        execution_relief = dict(signal.metadata.get("execution_relief_flags") or {})
        open_spike_guard = dict(signal.metadata.get("open_spike_guard") or {})
        higher_timeframe_guard = dict(signal.metadata.get("higher_timeframe_guard") or {})
        logger.info(
            f"[TradingCore] Decision {signal.asset} killed "
            f"step={signal.step_reached} dir={signal.direction} "
            f"predictor={self._fmt_predictor_pair(signal.metadata.get('predictor_prediction', signal.metadata.get('ml_prediction', context.get('predictor_prediction', context.get('ml_prediction')))), signal.metadata.get('predictor_confidence', signal.metadata.get('ml_confidence', context.get('predictor_confidence', context.get('ml_confidence')))))} "
            f"sent={self._fmt_metric(signal.metadata.get('sentiment_score', context.get('sentiment_score')))} "
            f"whale={signal.metadata.get('whale_dominant', 'n/a')} "
            f"oflow={self._fmt_metric(signal.metadata.get('orderflow_imbalance'))} "
            f"agent={self._fmt_metric(signal.metadata.get('agent_score'))} "
            f"final_conf={self._fmt_metric(signal.confidence)} "
            f"entry_style={str(signal.metadata.get('playbook_entry_style') or 'n/a').lower()} "
            f"generic_flow_override={str(signal.metadata.get('generic_flow_override_source') or 'off').lower()} "
            f"depth_sovereignty_source={str(execution_relief.get('depth_sovereignty_source') or 'n/a').lower()} "
            f"depth_sovereignty_reason={str(execution_relief.get('depth_sovereignty_reason') or 'n/a').lower()} "
            f"htf_action={str(higher_timeframe_guard.get('action') or 'n/a').lower()} "
            f"open_spike_action={str(open_spike_guard.get('action') or 'n/a').lower()} "
            f"late_risk={self._fmt_metric(signal.metadata.get('late_entry_risk_score'))} "
            f"blocks={self._fmt_reason_list(signal.metadata.get('execution_hard_blocks'), limit=3)} "
            f"late_reasons={self._fmt_reason_list(signal.metadata.get('late_entry_risk_reasons'), limit=3)} "
            f"reason={reason}"
        )

    @staticmethod
    def _initialize_playbook_runtime_seed(context: Dict[str, Any]) -> None:
        context.pop("ml_prediction", None)
        context.pop("ml_confidence", None)
        context["predictor_prediction"] = None
        context["predictor_confidence"] = 0.0
        context["predictor_model"] = ""
        context["predictor_provider"] = ""
        context["seed_decision"] = {
            "status": "playbook_runtime",
            "model": "playbook",
            "probability": None,
            "confidence": 0.0,
            "reason": "playbook runtime active; no external predictor attached",
        }

    @staticmethod
    def _extract_seed_structure_metrics(structure: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "structure_bias": str(structure.get("structure_bias", "neutral")).lower(),
            "alignment_score": float(structure.get("alignment_score", 0.0) or 0.0),
            "setup_quality": float(structure.get("setup_quality", 0.0) or 0.0),
            "pullback_score": float(structure.get("pullback_score", 0.0) or 0.0),
            "breakout_score": float(structure.get("breakout_score", 0.0) or 0.0),
            "volatility_state": str(structure.get("volatility_state", "unknown")),
        }

    @staticmethod
    def _build_playbook_decision_snapshot(
        playbook_pick: Dict[str, Any],
        playbook_primary: Dict[str, Any],
        *,
        playbook_action: str,
        playbook_direction: str,
        playbook_name: str,
        playbook_confidence: float,
        playbook_entry_style: str,
        playbook_interval: str,
    ) -> Dict[str, Any]:
        decision_context = playbook_primary if playbook_primary else playbook_pick
        return {
            "action": playbook_action,
            "playbook": playbook_name,
            "direction": playbook_direction,
            "confidence": round(playbook_confidence, 4),
            "score": round(float(decision_context.get("score", 0.0) or 0.0), 4),
            "context_confluence": round(float(decision_context.get("context_confluence", 0.0) or 0.0), 4),
            "cross_alignment": round(float(decision_context.get("cross_alignment", decision_context.get("cross_context_support", 0.0)) or 0.0), 4),
            "cross_confidence": round(float(decision_context.get("cross_confidence", 0.0) or 0.0), 4),
            "micro_score": round(float(decision_context.get("micro_score", decision_context.get("micro_context_support", 0.0)) or 0.0), 4),
            "whale_context_support": round(float(decision_context.get("whale_context_support", 0.0) or 0.0), 4),
            "shock_score": round(float(decision_context.get("shock_score", 0.0) or 0.0), 4),
            "shock_event_score": round(float(decision_context.get("shock_event_score", 0.0) or 0.0), 4),
            "headline_shock_score": round(float(decision_context.get("headline_shock_score", 0.0) or 0.0), 4),
            "shock_displacement_score": round(float(decision_context.get("shock_displacement_score", 0.0) or 0.0), 4),
            "shock_structure_score": round(float(decision_context.get("shock_structure_score", 0.0) or 0.0), 4),
            "shock_liquidity_score": round(float(decision_context.get("shock_liquidity_score", 0.0) or 0.0), 4),
            "shock_timing_score": round(float(decision_context.get("shock_timing_score", 0.0) or 0.0), 4),
            "shock_fresh_event": bool(decision_context.get("shock_fresh_event")),
            "shock_supported": bool(decision_context.get("shock_supported")),
            "shock_event_label": str(decision_context.get("shock_event_label") or ""),
            "support_components": int(decision_context.get("support_components", 0) or 0),
            "conflict_components": int(decision_context.get("conflict_components", 0) or 0),
            "entry_style": playbook_entry_style,
            "session": str(playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "session_label": str(playbook_pick.get("session_label") or playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "preferred_interval": playbook_interval,
            "candidate_count": len(playbook_pick.get("candidates") or []),
            "blocked_reason": str(playbook_pick.get("blocked_reason") or ""),
            "rejected_reasons": list(playbook_pick.get("rejected_reasons") or []),
            "rejected_details": list(playbook_pick.get("rejected_details") or []),
            "generic_flow_override": bool(decision_context.get("generic_flow_override")),
            "generic_flow_override_source": str(decision_context.get("generic_flow_override_source") or ""),
            "inactivity_profile": dict(playbook_pick.get("inactivity_profile") or {}),
            "allowed_sessions": list(playbook_pick.get("allowed_sessions") or []),
            "asset_plan": dict(playbook_pick.get("asset_plan") or {}),
            "notes": list(decision_context.get("notes") or []),
        }

    def _resolve_playbook_seed_selection(
        self,
        *,
        asset: str,
        context: Dict[str, Any],
        playbook_pick: Dict[str, Any],
        playbook_action: str,
        playbook_direction: str,
        playbook_name: str,
        playbook_confidence: float,
        playbook_interval: str,
        structure_bias: str,
        alignment_score: float,
        setup_quality: float,
    ) -> Optional[Dict[str, Any]]:
        if playbook_action in {"seed", "override"} and playbook_direction:
            context["seed_decision"].update(
                {
                    "status": "playbook_seed",
                    "reason": playbook_action,
                    "direction": playbook_direction,
                    "playbook": playbook_name,
                    "playbook_confidence": round(playbook_confidence, 4),
                }
            )
            return {
                "direction": playbook_direction,
                "seed_confidence": playbook_confidence,
                "seed_source": "playbook",
                "seed_model": playbook_name or "playbook",
            }

        rejection_reason = str(playbook_pick.get("blocked_reason") or "no_playbook_seed")
        context["seed_decision"].update(
            {
                "status": "rejected",
                "reason": rejection_reason,
                "session": context["playbook_decision"].get("session_label")
                or context["playbook_decision"].get("session")
                or "",
                "playbook_timeframe": playbook_interval or str(context.get("timeframe") or ""),
                "candidate_count": context["playbook_decision"].get("candidate_count", 0),
                "rejected_reasons": list(context["playbook_decision"].get("rejected_reasons") or []),
                "rejected_details": list(context["playbook_decision"].get("rejected_details") or []),
                "structure_bias": structure_bias,
                "alignment_score": round(alignment_score, 4),
                "setup_quality": round(setup_quality, 4),
            }
        )
        self._log_seed_decision(asset, context, rejection_reason)
        return None

    @staticmethod
    def _apply_seed_structure_adjustments(
        *,
        direction: str,
        seed_confidence: float,
        structure_bias: str,
        alignment_score: float,
        setup_quality: float,
        pullback_score: float,
        breakout_score: float,
    ) -> Tuple[float, str]:
        structure_note = "neutral"
        direction_sign = 1 if direction == "BUY" else -1
        setup_alignment = breakout_score if abs(breakout_score) >= abs(pullback_score) else pullback_score

        if structure_bias in {"buy", "sell"}:
            if (structure_bias == "buy" and direction == "BUY") or (structure_bias == "sell" and direction == "SELL"):
                boost = min(0.08, 0.02 + alignment_score * 0.04 + max(0.0, setup_quality - 0.25) * 0.05)
                seed_confidence = min(MAX_SIGNAL_CONFIDENCE, seed_confidence + boost)
                structure_note = "aligned"
            else:
                penalty = min(0.12, 0.03 + alignment_score * 0.05 + max(0.0, setup_quality - 0.20) * 0.04)
                seed_confidence = max(0.0, seed_confidence - penalty)
                structure_note = "conflict"

        if setup_alignment * direction_sign >= 0.40:
            seed_confidence = min(MAX_SIGNAL_CONFIDENCE, seed_confidence + min(0.03, abs(setup_alignment) * 0.03))
        elif setup_alignment * direction_sign <= -0.40:
            seed_confidence = max(0.0, seed_confidence - min(0.04, abs(setup_alignment) * 0.04))
        return seed_confidence, structure_note

    def _update_seed_signal_context_metadata(
        self,
        *,
        context: Dict[str, Any],
        structure: Dict[str, Any],
        playbook_action: str,
        playbook_name: str,
        playbook_direction: str,
        playbook_confidence: float,
        playbook_entry_style: str,
        playbook_interval: str,
        seed_confidence: float,
        structure_bias: str,
        alignment_score: float,
        setup_quality: float,
        pullback_score: float,
        breakout_score: float,
        volatility_state: str,
    ) -> None:
        existing_meta = dict(context.get("signal_metadata") or {})
        predictor = self._extract_predictor_runtime(context)
        context["signal_metadata"] = {
            **existing_meta,
            "playbook_action": playbook_action,
            "playbook_name": playbook_name,
            "playbook_direction": playbook_direction,
            "playbook_confidence": round(playbook_confidence, 4),
            "playbook_entry_style": playbook_entry_style,
            "playbook_timeframe": playbook_interval or str(context.get("timeframe") or ""),
            "shock_score": round(float((context.get("playbook_decision") or {}).get("shock_score", 0.0) or 0.0), 4),
            "shock_event_score": round(float((context.get("playbook_decision") or {}).get("shock_event_score", 0.0) or 0.0), 4),
            "headline_shock_score": round(float((context.get("playbook_decision") or {}).get("headline_shock_score", 0.0) or 0.0), 4),
            "shock_displacement_score": round(float((context.get("playbook_decision") or {}).get("shock_displacement_score", 0.0) or 0.0), 4),
            "shock_structure_score": round(float((context.get("playbook_decision") or {}).get("shock_structure_score", 0.0) or 0.0), 4),
            "shock_liquidity_score": round(float((context.get("playbook_decision") or {}).get("shock_liquidity_score", 0.0) or 0.0), 4),
            "shock_timing_score": round(float((context.get("playbook_decision") or {}).get("shock_timing_score", 0.0) or 0.0), 4),
            "shock_fresh_event": bool((context.get("playbook_decision") or {}).get("shock_fresh_event")),
            "shock_supported": bool((context.get("playbook_decision") or {}).get("shock_supported")),
            "shock_event_label": str((context.get("playbook_decision") or {}).get("shock_event_label") or ""),
            "sentiment_score": context.get("sentiment_score", 0.0),
            "regime": structure.get("regime", context.get("regime", "unknown")),
            "confidence": seed_confidence,
            "structure_bias": structure_bias,
            "alignment_score": round(alignment_score, 4),
            "setup_quality": round(setup_quality, 4),
            "pullback_score": round(pullback_score, 4),
            "breakout_score": round(breakout_score, 4),
            "volatility_state": volatility_state,
        }
        if predictor["available"]:
            context["signal_metadata"].update(
                {
                    "predictor_prediction": round(float(predictor["prediction"]), 4),
                    "predictor_confidence": round(float(predictor["confidence"]), 4),
                    "predictor_real": True,
                    "predictor_model": predictor["model"],
                    "predictor_provider": predictor["provider"],
                    "ml_prediction": round(float(predictor["prediction"]), 4),
                    "ml_confidence": round(float(predictor["confidence"]), 4),
                    "ml_prediction_real": True,
                }
            )

    def _reject_seed_signal(self, asset: str, context: Dict[str, Any], reason: str) -> None:
        context["seed_decision"]["status"] = "rejected"
        context["seed_decision"]["reason"] = reason
        self._log_seed_decision(asset, context, reason)

    def _extract_seed_entry_price(
        self,
        asset: str,
        category: str,
        direction: str,
        playbook_name: str,
        playbook_entry_style: str,
        price_data,
        playbook_price_data,
        structure: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        signal_frame = playbook_price_data
        if signal_frame is None or getattr(signal_frame, "empty", True):
            signal_frame = price_data

        try:
            signal_price = float(signal_frame["close"].iloc[-1])
        except Exception:
            self._reject_seed_signal(asset, context, "invalid_entry_price")
            return None

        current_price = float(context.get("current_price", 0.0) or 0.0)
        entry_price = current_price if current_price > 0.0 else signal_price
        if entry_price <= 0.0:
            self._reject_seed_signal(asset, context, "non_positive_entry_price")
            return None

        structure = structure if isinstance(structure, dict) else {}
        atr = self._estimate_atr(signal_frame)
        atr_unit = max(float(atr or 0.0), entry_price * 0.0015, 1e-9)
        asset_category = str(category or "").strip().lower()
        entry_style_label = str(playbook_entry_style or "").strip().lower()
        playbook_label = str(playbook_name or "").strip().lower()

        def _structure_levels(*keys: str) -> List[float]:
            levels: List[float] = []
            for key in keys:
                raw = structure.get(key)
                if isinstance(raw, (int, float)):
                    level = float(raw)
                    if level > 0:
                        levels.append(level)
                elif isinstance(raw, (list, tuple)):
                    for item in raw:
                        try:
                            level = float(item)
                        except Exception:
                            continue
                        if level > 0:
                            levels.append(level)
            return levels

        def _nearest_relative_level(relation: str, *keys: str) -> float:
            levels = _structure_levels(*keys)
            if not levels:
                return 0.0
            if relation == "below":
                candidates = [level for level in levels if level < entry_price]
                return max(candidates) if candidates else 0.0
            if relation == "above":
                candidates = [level for level in levels if level > entry_price]
                return min(candidates) if candidates else 0.0
            return min(levels, key=lambda level: abs(level - entry_price))

        anchor_price = 0.0
        anchor_role = ""
        anchor_distance_atr = 0.0
        confidence_penalty = 0.0
        stale_limit_atr = 0.60
        signal_drift_atr = abs(entry_price - signal_price) / atr_unit if signal_price > 0 else 0.0
        inactivity_profile = _seed_inactivity_profile(context)
        inactivity_relief_strength = float(inactivity_profile.get("relief_strength", 0.0) or 0.0)
        inactivity_entry_relief = bool(inactivity_profile.get("active")) and (
            bool(inactivity_profile.get("flat_book")) or bool(inactivity_profile.get("equity_relief"))
        ) and inactivity_relief_strength > 0.0
        strong_seed_context = bool(
            float(structure.get("alignment_score", 0.0) or 0.0) >= 0.72
            and float(structure.get("setup_quality", 0.0) or 0.0) >= 0.66
            and float((context.get("playbook_decision") or {}).get("confidence", 0.0) or 0.0) >= 0.66
        )
        playbook_decision = dict(context.get("playbook_decision") or {})
        signal_metadata = dict(context.get("signal_metadata") or {})
        microstructure: Dict[str, Any] = {}
        signal_micro = signal_metadata.get("market_microstructure")
        if isinstance(signal_micro, dict):
            microstructure.update(signal_micro)
        context_micro = context.get("market_microstructure")
        if isinstance(context_micro, dict):
            microstructure.update(context_micro)

        def _entry_float(value: Any, default: float = 0.0) -> float:
            try:
                return float(value if value not in (None, "") else default)
            except Exception:
                return float(default)

        def _entry_int(value: Any, default: int = 0) -> int:
            try:
                return int(value if value not in (None, "") else default)
            except Exception:
                return int(default)

        direction_sign = 1.0 if str(direction or "").upper() == "BUY" else -1.0 if str(direction or "").upper() == "SELL" else 0.0
        provider = str(
            microstructure.get("depth_provider")
            or microstructure.get("provider")
            or microstructure.get("source")
            or microstructure.get("exchange")
            or ""
        ).strip().lower()
        provider_class = str(
            microstructure.get("depth_provider_class") or microstructure.get("source_class") or ""
        ).strip().lower()
        source = str(microstructure.get("microstructure_source") or "").strip().lower()
        environment = str(microstructure.get("depth_environment") or microstructure.get("environment") or "").strip().lower()
        update_mode = str(microstructure.get("depth_update_mode") or "").strip().lower()
        depth_available = bool(microstructure.get("depth_available"))
        synthetic_depth = bool(
            microstructure.get("synthetic_depth")
            or microstructure.get("synthetic_depth_available")
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
            or provider_class == "sidecar"
        )
        redis_depth = bool(provider_class == "redis_subscriber" or source == "order_flow_true_depth")
        depth_levels = _entry_int(
            microstructure.get("depth_levels")
            or max(
                _entry_int(microstructure.get("bid_level_count", microstructure.get("visible_bid_levels", 0))),
                _entry_int(microstructure.get("ask_level_count", microstructure.get("visible_ask_levels", 0))),
            )
        )
        depth_quality = max(0.0, min(1.0, _entry_float(microstructure.get("depth_quality"), 0.0)))
        if depth_quality <= 0.0 and depth_levels > 0:
            depth_quality = (
                1.0
                if depth_levels >= 50
                else 0.82
                if depth_levels >= 8
                else 0.66
                if depth_levels >= 6
                else 0.48
                if depth_levels >= 4
                else 0.30
            )
        provider_trust = max(0.0, min(1.0, _entry_float(microstructure.get("depth_provider_trust_score"), 0.0)))
        if provider_trust <= 0.0:
            if exchange_depth:
                provider_trust = 0.86
            elif "dukascopy" in provider:
                provider_trust = 0.92
            elif "ctrader" in provider:
                provider_trust = 0.58 if environment and environment != "live" else 0.78
            elif redis_depth:
                provider_trust = 0.90
        quote_alignment = max(0.0, min(1.0, _entry_float(microstructure.get("depth_quote_alignment_score"), 0.0)))
        external_depth_rejected = bool(microstructure.get("external_depth_rejected"))
        quote_state = str(microstructure.get("depth_quote_agreement_state") or "").strip().lower()
        if quote_alignment <= 0.0 and depth_available and not external_depth_rejected:
            quote_alignment = 0.86 if exchange_depth else 0.80 if sidecar_depth or redis_depth else 0.74
        dom_ladder_ready = bool(microstructure.get("dom_ladder_ready"))
        dom_stream_snapshot_ready = bool(microstructure.get("dom_stream_snapshot_ready"))
        dom_stream_degraded = bool(microstructure.get("dom_stream_degraded"))
        dom_stream_health_score = max(0.0, min(1.0, _entry_float(microstructure.get("dom_stream_health_score"), 1.0)))
        min_depth_levels = 50 if exchange_depth else 8 if redis_depth else 2 if sidecar_depth else 4
        min_depth_quality = 0.74 if exchange_depth and (dom_stream_degraded or dom_ladder_ready) else 0.45 if exchange_depth or redis_depth else 0.25 if sidecar_depth else 0.35
        min_provider_trust = 0.82 if exchange_depth and (dom_stream_degraded or dom_ladder_ready) else 0.72 if exchange_depth or redis_depth else 0.60 if sidecar_depth else 0.64
        real_dom_entry_ready = bool(
            depth_available
            and not synthetic_depth
            and not external_depth_rejected
            and quote_state not in {"divergent", "severe_divergence"}
            and (exchange_depth or sidecar_depth or redis_depth)
            and depth_levels >= min_depth_levels
            and depth_quality >= min_depth_quality
            and provider_trust >= min_provider_trust
            and quote_alignment >= 0.80
            and update_mode not in {"none", "synthetic", "top_quote", "top_of_book"}
            and (
                not dom_ladder_ready
                or dom_stream_snapshot_ready
                or update_mode in {"snapshot_poll", "stream_snapshot", "snapshot_stream"}
                or dom_stream_health_score >= 0.45
            )
        )
        aligned_micro_flow = 0.0
        if direction_sign:
            aligned_micro_flow = max(
                _entry_float(microstructure.get("score"), 0.0) * direction_sign,
                _entry_float(microstructure.get("trade_flow_score", microstructure.get("score")), 0.0) * direction_sign,
                _entry_float(microstructure.get("orderflow_score"), 0.0) * direction_sign,
                _entry_float(microstructure.get("book_imbalance"), 0.0) * direction_sign * 0.90,
                _entry_float(microstructure.get("orderflow_book_imbalance"), 0.0) * direction_sign * 0.85,
                (_entry_float(microstructure.get("velocity_bps"), 0.0) * direction_sign / 0.45) * 0.85,
            )
        breakout_ignition_entry = bool(
            real_dom_entry_ready
            and (
                "breakout" in entry_style_label
                or playbook_label in {
                    "aggressive_expansion",
                    "breakout_continuation",
                    "crypto_orderflow_continuation",
                    "intermarket_continuation",
                    "news_impulse",
                    "opening_drive",
                }
            )
            and float(structure.get("alignment_score", 0.0) or 0.0) >= 0.58
            and float(structure.get("setup_quality", 0.0) or 0.0) >= 0.54
            and float(structure.get("target_efficiency_score", 0.0) or 0.0) >= 0.08
            and float(structure.get("extension_score", 0.0) or 0.0) <= 1.62
            and int(structure.get("impulse_age_bars", 0) or 0) <= 8
            and float(structure.get("cluster_penalty", 0.0) or 0.0) <= 0.20
            and int(playbook_decision.get("support_components", 0) or 0) >= 1
            and int(playbook_decision.get("conflict_components", 0) or 0) == 0
            and (
                float(playbook_decision.get("confidence", 0.0) or 0.0) >= 0.66
                or float(playbook_decision.get("context_confluence", 0.0) or 0.0) >= 0.18
                or float(playbook_decision.get("micro_score", 0.0) or 0.0) >= 0.24
                or aligned_micro_flow >= 0.24
            )
        )

        if "retest" in entry_style_label or playbook_label == "breakout_retest":
            anchor_role = "retest_level"
            if direction == "BUY":
                anchor_price = _nearest_relative_level("nearest", "resistance", "resistance_levels")
            else:
                anchor_price = _nearest_relative_level("nearest", "support", "support_levels")
            stale_limit_atr = 0.48
        elif "pullback" in entry_style_label or playbook_label == "trend_pullback":
            anchor_role = "pullback_anchor"
            if direction == "BUY":
                anchor_price = _nearest_relative_level("below", "support", "support_levels", "recent_low")
            else:
                anchor_price = _nearest_relative_level("above", "resistance", "resistance_levels", "recent_high")
            stale_limit_atr = 0.52
        elif "reclaim" in entry_style_label or playbook_label == "failed_break_reclaim":
            anchor_role = "reclaim_anchor"
            if direction == "BUY":
                anchor_price = _nearest_relative_level("nearest", "support", "support_levels", "resistance", "resistance_levels")
            else:
                anchor_price = _nearest_relative_level("nearest", "resistance", "resistance_levels", "support", "support_levels")
            stale_limit_atr = 0.48
        elif playbook_label in {"news_impulse", "aggressive_expansion", "opening_drive", "crypto_orderflow_continuation"}:
            stale_limit_atr = 0.42
        elif playbook_label in {"breakout_continuation", "intermarket_continuation"}:
            stale_limit_atr = 0.50
        if breakout_ignition_entry:
            stale_limit_atr = max(stale_limit_atr, 0.84 if exchange_depth or redis_depth else 0.76)

        if anchor_role:
            if anchor_price <= 0.0:
                self._reject_seed_signal(asset, context, f"missing_{anchor_role}")
                return None
            anchor_distance_atr = abs(entry_price - anchor_price) / atr_unit
            if anchor_role == "pullback_anchor":
                # Forex pullbacks can stay structurally valid a little farther away
                # from the anchor than the tighter cross-asset default.
                anchor_tolerance_atr = 0.38 if asset_category == "forex" else 0.30
                if (
                    bool(structure.get("first_pullback_ready"))
                    and bool(
                        structure.get("fast_entry_confirmation_ready")
                        or structure.get("entry_confirmation_ready")
                    )
                ):
                    anchor_tolerance_atr += 0.08 if asset_category == "forex" else 0.06
            else:
                anchor_tolerance_atr = 0.26
            inactivity_anchor_tolerance = anchor_tolerance_atr
            if inactivity_entry_relief and strong_seed_context:
                inactivity_anchor_tolerance += 0.04 + inactivity_relief_strength * 0.10
            if anchor_distance_atr > inactivity_anchor_tolerance:
                self._reject_seed_signal(asset, context, f"{anchor_role}_not_in_range")
                return None
            if anchor_distance_atr > anchor_tolerance_atr and inactivity_entry_relief and strong_seed_context:
                confidence_penalty += min(0.09, (anchor_distance_atr - anchor_tolerance_atr) * (0.24 + inactivity_relief_strength * 0.10))
            confidence_penalty += min(0.07, max(0.0, anchor_distance_atr - 0.10) * 0.18)

        inactivity_stale_limit_atr = stale_limit_atr
        if inactivity_entry_relief and strong_seed_context:
            inactivity_stale_limit_atr += 0.05 + inactivity_relief_strength * 0.12
        if signal_drift_atr > inactivity_stale_limit_atr:
            self._reject_seed_signal(asset, context, "stale_market_entry")
            return None
        if signal_drift_atr > stale_limit_atr and inactivity_entry_relief and strong_seed_context:
            confidence_penalty += min(0.08, (signal_drift_atr - stale_limit_atr) * (0.20 + inactivity_relief_strength * 0.10))
        elif breakout_ignition_entry and signal_drift_atr > 0.50:
            confidence_penalty += min(0.06, (signal_drift_atr - 0.50) * 0.12)
        confidence_penalty += min(0.08, max(0.0, signal_drift_atr - 0.16) * 0.14)

        return {
            "entry_price": round(entry_price, 6),
            "signal_price": round(signal_price, 6),
            "current_price": round(current_price, 6) if current_price > 0 else 0.0,
            "signal_atr": round(float(atr or 0.0), 6),
            "signal_drift_atr": round(signal_drift_atr, 4),
            "anchor_price": round(anchor_price, 6) if anchor_price > 0 else 0.0,
            "anchor_role": anchor_role,
            "anchor_distance_atr": round(anchor_distance_atr, 4),
            "stale_limit_atr": round(stale_limit_atr, 4),
            "inactivity_entry_relief": inactivity_entry_relief and strong_seed_context,
            "inactivity_relief_strength": round(inactivity_relief_strength, 4),
            "breakout_ignition_entry": breakout_ignition_entry,
            "real_dom_entry_ready": real_dom_entry_ready,
            "real_dom_entry_kind": "exchange" if exchange_depth else "sidecar" if sidecar_depth else "redis" if redis_depth else "none",
            "confidence_penalty": round(confidence_penalty, 4),
            "entry_source": "live_price" if current_price > 0 else "signal_close",
        }

    @staticmethod
    def _build_seed_signal_object(
        *,
        asset: str,
        canonical: str,
        category: str,
        direction: str,
        seed_confidence: float,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        playbook_name: str,
        playbook_action: str,
        seed_source: str,
        seed_model: str,
        take_profit_levels: List[float],
    ) -> Signal:
        return Signal(
            asset=asset,
            canonical_asset=canonical,
            category=category,
            direction=direction,
            confidence=round(min(MAX_SIGNAL_CONFIDENCE, max(0.0, seed_confidence)), 4),
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            risk_reward=round(abs(take_profit - entry_price) / max(abs(entry_price - stop_loss), 1e-9), 4),
            strategy_id=f"playbook_{playbook_name}" if playbook_name and playbook_action else "playbook_runtime",
            indicators={"seed_source": seed_source, "seed_model": seed_model},
            take_profit_levels=take_profit_levels,
        )

    @staticmethod
    def _unpack_seed_exit_plan(exit_plan: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "atr": float(exit_plan["atr"]),
            "stop_loss": float(exit_plan["stop_loss"]),
            "take_profit": float(exit_plan["take_profit"]),
            "take_profit_levels": list(exit_plan["take_profit_levels"]),
            "trade_management_plan": dict(exit_plan["trade_management_plan"]),
            "execution_feedback_policy": dict(exit_plan["execution_feedback_policy"]),
            "target_rr_multiplier": float(exit_plan["target_rr_multiplier"]),
            "stop_buffer_multiplier": float(exit_plan["stop_buffer_multiplier"]),
            "structure_target_alignment": dict(exit_plan["structure_target_alignment"]),
        }

    def _finalize_seed_signal_context(
        self,
        *,
        context: Dict[str, Any],
        direction: str,
        seed_confidence: float,
        structure_bias: str,
        structure_note: str,
        setup_quality: float,
        seed_source: str,
        seed_model: str,
        execution_feedback_policy: Dict[str, Any],
    ) -> None:
        context["execution_feedback_policy"] = execution_feedback_policy
        context["seed_decision"]["status"] = "signal"
        context["seed_decision"]["direction"] = direction
        context["seed_decision"]["confidence"] = round(seed_confidence, 4)
        context["seed_decision"]["structure_bias"] = structure_bias
        context["seed_decision"]["structure_note"] = structure_note
        context["seed_decision"]["setup_quality"] = round(setup_quality, 4)
        context["seed_decision"]["seed_source"] = seed_source
        context["seed_decision"]["seed_model"] = seed_model
        context["seed_decision"]["execution_feedback_policy"] = execution_feedback_policy

    def _generate_seed_signal(
        self,
        asset: str,
        canonical: str,
        category: str,
        price_data,
        context: Dict[str, Any],
    ) -> Optional[Signal]:
        self._initialize_playbook_runtime_seed(context)

        structure = context.get("market_structure") or {}
        structure_metrics = self._extract_seed_structure_metrics(structure)
        structure_bias = str(structure_metrics["structure_bias"])
        alignment_score = float(structure_metrics["alignment_score"])
        setup_quality = float(structure_metrics["setup_quality"])
        pullback_score = float(structure_metrics["pullback_score"])
        breakout_score = float(structure_metrics["breakout_score"])
        volatility_state = str(structure_metrics["volatility_state"])

        playbook_pick, playbook_interval, playbook_price_data = self._resolve_playbook_seed(
            asset,
            canonical,
            category,
            price_data,
            context,
        )

        playbook_primary = dict(playbook_pick.get("primary") or {})
        playbook_action = str(playbook_pick.get("action") or "")
        playbook_direction = str(playbook_primary.get("direction") or "").upper()
        playbook_name = str(playbook_primary.get("playbook") or "").strip()
        playbook_confidence = float(playbook_primary.get("confidence", 0.0) or 0.0)
        playbook_entry_style = str(playbook_primary.get("entry_style") or "").strip()
        playbook_management_template = dict(playbook_primary.get("management") or {})
        context["playbook_decision"] = self._build_playbook_decision_snapshot(
            playbook_pick,
            playbook_primary,
            playbook_action=playbook_action,
            playbook_direction=playbook_direction,
            playbook_name=playbook_name,
            playbook_confidence=playbook_confidence,
            playbook_entry_style=playbook_entry_style,
            playbook_interval=playbook_interval,
        )

        seed_selection = self._resolve_playbook_seed_selection(
            asset=asset,
            context=context,
            playbook_pick=playbook_pick,
            playbook_action=playbook_action,
            playbook_direction=playbook_direction,
            playbook_name=playbook_name,
            playbook_confidence=playbook_confidence,
            playbook_interval=playbook_interval,
            structure_bias=structure_bias,
            alignment_score=alignment_score,
            setup_quality=setup_quality,
        )
        if seed_selection is None:
            return None

        direction = str(seed_selection["direction"])
        seed_confidence = float(seed_selection["seed_confidence"])
        seed_source = str(seed_selection["seed_source"])
        seed_model = str(seed_selection["seed_model"])
        seed_confidence, structure_note = self._apply_seed_structure_adjustments(
            direction=direction,
            seed_confidence=seed_confidence,
            structure_bias=structure_bias,
            alignment_score=alignment_score,
            setup_quality=setup_quality,
            pullback_score=pullback_score,
            breakout_score=breakout_score,
        )

        if setup_quality < 0.15 and seed_confidence < 0.18:
            self._reject_seed_signal(asset, context, "weak_structure_quality")
            return None

        self._update_seed_signal_context_metadata(
            context=context,
            structure=structure if isinstance(structure, dict) else {},
            playbook_action=playbook_action,
            playbook_name=playbook_name,
            playbook_direction=playbook_direction,
            playbook_confidence=playbook_confidence,
            playbook_entry_style=playbook_entry_style,
            playbook_interval=playbook_interval,
            seed_confidence=seed_confidence,
            structure_bias=structure_bias,
            alignment_score=alignment_score,
            setup_quality=setup_quality,
            pullback_score=pullback_score,
            breakout_score=breakout_score,
            volatility_state=volatility_state,
        )

        entry_plan = self._extract_seed_entry_price(
            asset,
            category=category,
            direction=direction,
            playbook_name=playbook_name,
            playbook_entry_style=playbook_entry_style,
            price_data=price_data,
            playbook_price_data=playbook_price_data,
            structure=structure if isinstance(structure, dict) else {},
            context=context,
        )
        if entry_plan is None:
            return None
        entry_price = float(entry_plan.get("entry_price", 0.0) or 0.0)
        seed_confidence = max(
            0.0,
            seed_confidence - float(entry_plan.get("confidence_penalty", 0.0) or 0.0),
        )
        exit_price_data = (
            playbook_price_data
            if playbook_price_data is not None and not getattr(playbook_price_data, "empty", True)
            else price_data
        )

        exit_plan = self._build_seed_exit_plan(
            asset=asset,
            canonical=canonical,
            category=category,
            direction=direction,
            entry_price=entry_price,
            price_data=exit_price_data,
            context=context,
            structure=structure if isinstance(structure, dict) else {},
            seed_confidence=seed_confidence,
            playbook_direction=playbook_direction,
            playbook_action=playbook_action,
            playbook_management_template=playbook_management_template,
            playbook_interval=playbook_interval,
            playbook_entry_style=playbook_entry_style,
            playbook_pick=playbook_pick,
            playbook_primary=playbook_primary,
        )
        unpacked_exit_plan = self._unpack_seed_exit_plan(exit_plan)
        atr = float(unpacked_exit_plan["atr"])
        stop_loss = float(unpacked_exit_plan["stop_loss"])
        take_profit = float(unpacked_exit_plan["take_profit"])
        take_profit_levels = list(unpacked_exit_plan["take_profit_levels"])
        trade_management_plan = dict(unpacked_exit_plan["trade_management_plan"])
        execution_feedback_policy = dict(unpacked_exit_plan["execution_feedback_policy"])
        target_rr_multiplier = float(unpacked_exit_plan["target_rr_multiplier"])
        stop_buffer_multiplier = float(unpacked_exit_plan["stop_buffer_multiplier"])
        structure_target_alignment = dict(unpacked_exit_plan["structure_target_alignment"])

        signal = self._build_seed_signal_object(
            asset=asset,
            canonical=canonical,
            category=category,
            direction=direction,
            seed_confidence=seed_confidence,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            playbook_name=playbook_name,
            playbook_action=playbook_action,
            seed_source=seed_source,
            seed_model=seed_model,
            take_profit_levels=take_profit_levels,
        )
        predictor = self._extract_predictor_runtime(context)
        signal.metadata.update({
            "seed_candidate_score": round(signal.confidence, 4),
            "seed_source": seed_source,
            "seed_model": seed_model,
            "playbook_action": playbook_action,
            "playbook_name": playbook_name,
            "playbook_direction": playbook_direction,
            "playbook_score": round(float(playbook_primary.get("score", 0.0) or 0.0), 4),
            "playbook_confidence": round(playbook_confidence, 4),
            "playbook_entry_style": playbook_entry_style,
            "playbook_session": str(playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "session_label": str(playbook_pick.get("session_label") or playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "playbook_timeframe": playbook_interval or str(context.get("timeframe") or ""),
            "playbook_notes": list(playbook_primary.get("notes") or []),
            "playbook_context_confluence": round(float(playbook_primary.get("context_confluence", 0.0) or 0.0), 4),
            "playbook_cross_alignment": round(float(playbook_primary.get("cross_alignment", playbook_primary.get("cross_context_support", 0.0)) or 0.0), 4),
            "playbook_cross_confidence": round(float(playbook_primary.get("cross_confidence", 0.0) or 0.0), 4),
            "playbook_micro_score": round(float(playbook_primary.get("micro_score", playbook_primary.get("micro_context_support", 0.0)) or 0.0), 4),
            "playbook_whale_context_support": round(float(playbook_primary.get("whale_context_support", 0.0) or 0.0), 4),
            "playbook_support_components": int(playbook_primary.get("support_components", 0) or 0),
            "playbook_conflict_components": int(playbook_primary.get("conflict_components", 0) or 0),
            "generic_flow_override": bool(playbook_primary.get("generic_flow_override")),
            "generic_flow_override_source": str(playbook_primary.get("generic_flow_override_source") or ""),
            "trade_management_plan": trade_management_plan,
            "market_data": context.get("market_data", {}),
            "atr": round(atr, 6) if atr > 0 else 0.0,
            "exit_model": "atr" if atr > 0 else "category_fallback",
            "market_structure": dict(structure) if isinstance(structure, dict) else {},
            "structure_bias": structure_bias,
            "alignment_score": round(alignment_score, 4),
            "setup_quality": round(setup_quality, 4),
            "pullback_score": round(pullback_score, 4),
            "breakout_score": round(breakout_score, 4),
            "volatility_state": volatility_state,
            "seed_structure_note": structure_note,
            "execution_feedback_policy": execution_feedback_policy,
            "execution_quality_score": round(float(execution_feedback_policy.get("avg_quality_score", 50.0) or 50.0), 1),
            "execution_feedback_sample_count": int(execution_feedback_policy.get("sample_count", 0) or 0),
            "target_rr_multiplier": round(target_rr_multiplier, 4),
            "stop_buffer_multiplier": round(stop_buffer_multiplier, 4),
            "structure_target_alignment": structure_target_alignment,
            "entry_plan": dict(entry_plan),
        })
        if predictor["available"]:
            predictor_direction = "BUY" if float(predictor["prediction"]) > 0.5 else "SELL"
            signal.metadata.update(
                {
                    "predictor_prediction": round(float(predictor["prediction"]), 4),
                    "predictor_confidence": round(float(predictor["confidence"]), 4),
                    "predictor_real": True,
                    "predictor_direction": predictor_direction,
                    "predictor_direction_agrees": predictor_direction == direction,
                    "predictor_model": predictor["model"],
                    "predictor_provider": predictor["provider"],
                    "ml_prediction": round(float(predictor["prediction"]), 4),
                    "ml_confidence": round(float(predictor["confidence"]), 4),
                    "ml_prediction_real": True,
                    "ml_direction": predictor_direction,
                    "ml_direction_agrees": predictor_direction == direction,
                }
            )
        context["execution_feedback_policy"] = execution_feedback_policy
        context["signal_metadata"] = {
            **dict(context.get("signal_metadata") or {}),
            "confidence": round(seed_confidence, 4),
            "execution_quality_score": round(float(execution_feedback_policy.get("avg_quality_score", 50.0) or 50.0), 1),
            "execution_feedback_sample_count": int(execution_feedback_policy.get("sample_count", 0) or 0),
            "target_rr_multiplier": round(target_rr_multiplier, 4),
            "stop_buffer_multiplier": round(stop_buffer_multiplier, 4),
            "structure_target_alignment": structure_target_alignment,
            "playbook_timeframe": playbook_interval or str(context.get("timeframe") or ""),
            "playbook_entry_style": playbook_entry_style,
            "playbook_session": str(playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "session_label": str(playbook_pick.get("session_label") or playbook_pick.get("session") or playbook_primary.get("session") or ""),
            "generic_flow_override": bool(playbook_primary.get("generic_flow_override")),
            "generic_flow_override_source": str(playbook_primary.get("generic_flow_override_source") or ""),
            "shock_score": round(float((context.get("playbook_decision") or {}).get("shock_score", 0.0) or 0.0), 4),
            "shock_event_score": round(float((context.get("playbook_decision") or {}).get("shock_event_score", 0.0) or 0.0), 4),
            "headline_shock_score": round(float((context.get("playbook_decision") or {}).get("headline_shock_score", 0.0) or 0.0), 4),
            "shock_displacement_score": round(float((context.get("playbook_decision") or {}).get("shock_displacement_score", 0.0) or 0.0), 4),
            "shock_structure_score": round(float((context.get("playbook_decision") or {}).get("shock_structure_score", 0.0) or 0.0), 4),
            "shock_liquidity_score": round(float((context.get("playbook_decision") or {}).get("shock_liquidity_score", 0.0) or 0.0), 4),
            "shock_timing_score": round(float((context.get("playbook_decision") or {}).get("shock_timing_score", 0.0) or 0.0), 4),
            "shock_fresh_event": bool((context.get("playbook_decision") or {}).get("shock_fresh_event")),
            "shock_supported": bool((context.get("playbook_decision") or {}).get("shock_supported")),
            "shock_event_label": str((context.get("playbook_decision") or {}).get("shock_event_label") or ""),
            "trade_management_plan": trade_management_plan,
            "entry_plan": dict(entry_plan),
        }
        if predictor["available"]:
            context["signal_metadata"].update(
                {
                    "predictor_prediction": round(float(predictor["prediction"]), 4),
                    "predictor_confidence": round(float(predictor["confidence"]), 4),
                    "predictor_real": True,
                    "predictor_model": predictor["model"],
                    "predictor_provider": predictor["provider"],
                    "ml_prediction": round(float(predictor["prediction"]), 4),
                    "ml_confidence": round(float(predictor["confidence"]), 4),
                    "ml_prediction_real": True,
                }
            )
        context["seed_decision"]["entry_plan"] = dict(entry_plan)
        self._finalize_seed_signal_context(
            context=context,
            direction=direction,
            seed_confidence=seed_confidence,
            structure_bias=structure_bias,
            structure_note=structure_note,
            setup_quality=setup_quality,
            seed_source=seed_source,
            seed_model=seed_model,
            execution_feedback_policy=execution_feedback_policy,
        )
        return signal

    @staticmethod
    def _estimate_atr(price_data, period: int = 14) -> float:
        try:
            if price_data is None or len(price_data) < period + 1:
                return 0.0
            required = {"high", "low", "close"}
            if not required.issubset(set(price_data.columns)):
                return 0.0
            high = price_data["high"].astype(float)
            low = price_data["low"].astype(float)
            close = price_data["close"].astype(float)
            prev_close = close.shift(1)
            tr = pd.concat(
                [
                    high - low,
                    (high - prev_close).abs(),
                    (low - prev_close).abs(),
                ],
                axis=1,
            ).max(axis=1)
            atr = float(tr.tail(period).mean())
            if atr > 0.0:
                return atr
        except Exception as exc:
            logger.debug(f"[TradingCore] ATR estimation failed: {exc}")
        return 0.0

    @staticmethod
    def _get_structure_intervals(primary_interval: str) -> List[str]:
        base = str(primary_interval or "15m").lower()
        plans = {
            "1m": ["5m", "15m", "1h"],
            "5m": ["15m", "1h", "4h"],
            "15m": ["5m", "1h", "4h"],
            "30m": ["1h", "4h"],
            "1h": ["4h", "1d"],
            "4h": ["1d"],
            "1d": [],
        }
        return plans.get(base, ["1h", "4h"])

    def _attach_market_structure_context(
        self,
        context: Dict[str, Any],
        asset: str,
        category: str,
        primary_price_data,
    ) -> None:
        if primary_price_data is None or getattr(primary_price_data, "empty", True):
            context["market_structure"] = {}
            return

        timeframe = str(context.get("timeframe") or get_trading_timeframe(category)).lower()
        frames: Dict[str, Any] = {timeframe: primary_price_data}
        frame_lengths: Dict[str, int] = {}
        try:
            frame_lengths[timeframe] = int(len(primary_price_data))
        except Exception:
            frame_lengths[timeframe] = 0

        if self.fetcher is not None:
            for interval in self._get_structure_intervals(timeframe):
                if interval in frames:
                    continue
                try:
                    df = self.fetcher.get_ohlcv(
                        asset,
                        category,
                        interval=interval,
                        periods=max(60, get_timeframe_periods(interval)),
                    )
                    if df is not None and not df.empty:
                        frames[interval] = df
                        frame_lengths[interval] = int(len(df))
                except Exception as exc:
                    logger.debug(f"[TradingCore] Structure frame {asset} {interval}: {exc}")

        try:
            from services.market_structure_service import get_service as get_market_structure_service

            structure = get_market_structure_service().analyze(asset, category, frames, context=context)
        except Exception as exc:
            logger.debug(f"[TradingCore] Market structure build failed for {asset}: {exc}")
            structure = {}

        context["market_structure"] = structure
        context["structure_frame_lengths"] = frame_lengths
        if isinstance(structure, dict) and structure.get("regime"):
            context["regime"] = structure.get("regime", context.get("regime", "unknown"))

    def _get_prices(self) -> Dict[str, float]:
        prices = {}
        for pos in self.state.get_open_positions():
            asset    = pos.get("asset", "")
            category = pos.get("category", "forex")
            if asset and self.fetcher:
                try:
                    price, _ = self.fetcher.get_real_time_price(asset, category)
                    if price:
                        prices[asset] = price
                except Exception:
                    pass
        return prices

    # ── Context helpers — wired so playbooks receive live macro/narrative data ──

    @staticmethod
    def _get_macro_impact_static() -> str:
        """
        FIX: Read macro impact level from MacroDataCollector so the
        MarketConditionClassifier can detect crisis regimes.
        Previously this was never populated → always "LOW" → crisis unreachable.
        """
        try:
            from data_ingestion import macro_data_collector as _mdc
            collector = getattr(_mdc, "collector", None)
            if collector is not None:
                return getattr(collector, "current_impact", "LOW")
        except Exception:
            pass
        return "LOW"

    @staticmethod
    def _get_narrative_strength_static(asset: str) -> float:
        """
        FIX: Read narrative strength from Phase 4 TopicClusterEngine.
        Previously this was never populated → always 0.0 → narrative boost
        in Layer 5 never fired and crisis regime via narrative unreachable.
        """
        try:
            from narrative_ai import get_narrative_scores
            scores = get_narrative_scores()
            if scores:
                return round(max(scores.values()), 3)
        except Exception:
            pass
        return 0.0

    def _build_context(self, asset: str = "", category: str = "") -> Dict[str, Any]:
        intelligence_snapshot: Dict[str, Any] = {}
        try:
            from services.market_intelligence_service import get_service as get_market_intelligence_service

            intelligence_snapshot = get_market_intelligence_service().get_asset_snapshot(asset, category)
        except Exception:
            intelligence_snapshot = {}

        sentiment_details = intelligence_snapshot.get("sentiment_details") or {}
        sentiment_score = float(
            intelligence_snapshot.get(
                "sentiment_score",
                sentiment_details.get("composite_score", sentiment_details.get("score", 0.0)),
            ) or 0.0
        )
        free_market_intelligence = intelligence_snapshot.get("free_market_intelligence") or {}
        funding_bias = intelligence_snapshot.get("funding_bias", "NEUTRAL")
        oi_signal = intelligence_snapshot.get("oi_signal", "NEUTRAL")
        market_open, market_reason = self._market_hours_status(asset, category)

        return {
            "asset":              asset,
            "category":          category,
            "timeframe":         get_trading_timeframe(category),
            "balance":           self.state.balance,
            "open_count":        self.state.open_position_count(),
            "daily_pnl":         self.state.daily_pnl,
            "engine":            self,
            "fetcher":           self.fetcher,
            "market_intelligence": intelligence_snapshot,
            "market_status":     {
                "asset": asset,
                "market_open": market_open,
                "reason": market_reason,
            },
            "sentiment_score":   sentiment_score,
            "sentiment_details": sentiment_details,
            "free_market_intelligence": free_market_intelligence,
            "funding_bias":      funding_bias,    # Phase 1 → Layer 8 Meta AI
            "oi_signal":         oi_signal,       # Phase 1 → Layer 8 Meta AI
            "news_event":        _get_news_event(category),  # news event state
            "market_data":       {},
            "market_structure":  {},
            "broker_quality":    {},
            "cross_asset_context": {},
            "market_microstructure": {},
            # FIX: wire macro_impact from MacroDataCollector so crisis regime
            # can trigger in MarketConditionClassifier.classify().
            # Previously this key was never set → macro_impact always "LOW" →
            # crisis regime unreachable via macro path.
            "macro_impact":      self._get_macro_impact_static(),
            # FIX: wire narrative_strength from Phase 4 TopicClusterEngine so
            # the crisis regime check (macro_impact=HIGH AND narrative_str>0.3)
            # has a chance of firing.  Previously always 0.0.
            "narrative_strength": float(intelligence_snapshot.get("narrative_strength", self._get_narrative_strength_static(asset)) or 0.0),
            "dominant_narrative": intelligence_snapshot.get("dominant_narrative", ""),
        }

    def _attach_broker_quality_context(
        self,
        ctx: Dict[str, Any],
        asset: str,
        category: str,
        *,
        price: float,
        spread: float,
        price_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        if not self.fetcher:
            ctx["broker_quality"] = {}
            return
        try:
            from services.broker_quality_service import get_service as get_broker_quality_service

            ctx["broker_quality"] = get_broker_quality_service().build_snapshot(
                asset=asset,
                category=category,
                fetcher=self.fetcher,
                primary_price=price,
                primary_spread=spread,
                primary_meta=price_meta or self.fetcher.get_last_price_metadata(asset),
                market_status=ctx.get("market_status"),
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Broker quality context unavailable for {asset}: {exc}")
            ctx["broker_quality"] = {}

    def _attach_cross_asset_context(
        self,
        ctx: Dict[str, Any],
        asset: str,
        category: str,
        *,
        timeframe: str = "",
    ) -> None:
        if not self.fetcher:
            ctx["cross_asset_context"] = {}
            return
        try:
            from services.cross_asset_spillover_service import get_service as get_cross_asset_spillover_service

            ctx["cross_asset_context"] = get_cross_asset_spillover_service().build_snapshot(
                asset=asset,
                category=category,
                fetcher=self.fetcher,
                timeframe=str(timeframe or get_trading_timeframe(category) or "15m"),
            )
        except Exception as exc:
            logger.debug(f"[TradingCore] Cross-asset context unavailable for {asset}: {exc}")
            ctx["cross_asset_context"] = {}

    @staticmethod
    def _clean_broker_issue_reason(reason: Any, limit: int = 650) -> str:
        text = " ".join(str(reason or "unknown broker issue").split())
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 3)].rstrip() + "..."

    @staticmethod
    def _broker_issue_bucket(reason: Any) -> str:
        text = str(reason or "").lower()
        if not text.strip():
            return "unknown"
        checks = (
            ("rate_limit", ("exceeded-api-key-allowance", "allowance limits", "rate limit", "temporarily suspended")),
            ("permission_denied", ("broker_permission_denied", "unauthorised", "unauthorized", "no access")),
            ("missing_epic", ("epic not found", "epic not configured")),
            ("missing_contract_spec", ("contract spec missing",)),
            ("size_rejected", ("below_broker_min_size", "size rejected")),
            ("attached_order_error", ("attached_order_level_error",)),
            ("temporary_unavailable", ("broker_temporarily_unavailable", "temporarily unavailable")),
            ("confirm_pending", ("ig_confirm_pending", "confirm pending", "_confirm_pending")),
            ("market_closed", ("market closed", "market not open")),
            ("dry_run", ("dry-run", "dry run")),
            ("unknown_rejection", ("ig rejected order: unknown", " rejected order: unknown")),
        )
        for bucket, needles in checks:
            if any(needle in text for needle in needles):
                return bucket
        return "broker_rejection"

    @staticmethod
    def _broker_issue_is_expected_noise(bucket: str, reason: Any) -> bool:
        text = str(reason or "").lower()
        if bucket == "market_closed":
            return True
        if "closed on deriv" in text or "weekend closed" in text or "pre-market" in text:
            return True
        return False

    @staticmethod
    def _broker_issue_alert_ttl(bucket: str) -> float:
        if bucket in {"rate_limit", "temporary_unavailable", "confirm_pending"}:
            return 15.0 * 60.0
        return 6.0 * 60.0 * 60.0

    @staticmethod
    def _format_broker_issue_alert(issue: Dict[str, Any]) -> str:
        lines = [
            "Broker execution issue",
            f"Asset: {issue.get('asset') or '?'}",
            f"Category: {issue.get('category') or 'unknown'}",
            f"Stage: {issue.get('stage') or 'unknown'}",
            f"Mode: {issue.get('execution_mode') or EXECUTION_MODE or 'unknown'}",
            f"Bot action: {issue.get('action') or 'skipped/rejected'}",
            f"Reason: {issue.get('reason') or 'unknown'}",
        ]
        return "\n".join(lines)

    def _notify_broker_issue(
        self,
        asset: Any,
        category: Any,
        reason: Any,
        *,
        stage: str,
        action: str,
    ) -> None:
        if not self.telegram:
            return
        reason_text = self._clean_broker_issue_reason(reason)
        bucket = self._broker_issue_bucket(reason_text)
        if self._broker_issue_is_expected_noise(bucket, reason_text):
            return

        asset_key = str(asset or "?").upper()
        broker_scope_buckets = {"rate_limit", "temporary_unavailable", "confirm_pending"}
        alert_scope = "BROKER" if bucket in broker_scope_buckets else asset_key
        alert_key = f"{alert_scope}:{bucket}"
        now = time.monotonic()
        ttl = self._broker_issue_alert_ttl(bucket)
        last = float(self._broker_issue_alerts.get(alert_key) or 0.0)
        if last and now - last < ttl:
            return
        self._broker_issue_alerts[alert_key] = now
        if len(self._broker_issue_alerts) > 500:
            cutoff = now - 24.0 * 60.0 * 60.0
            self._broker_issue_alerts = {
                key: value
                for key, value in self._broker_issue_alerts.items()
                if float(value or 0.0) >= cutoff
            }

        issue = {
            "asset": str(asset or "?"),
            "category": str(category or "unknown"),
            "stage": stage,
            "action": action,
            "reason": reason_text,
            "bucket": bucket,
            "execution_mode": str(EXECUTION_MODE or "unknown"),
        }
        try:
            target = getattr(self.telegram, "bot", self.telegram)
            if hasattr(target, "alert_broker_issue"):
                target.alert_broker_issue(issue)
            elif hasattr(target, "send_message"):
                target.send_message(self._format_broker_issue_alert(issue), parse_mode=None)
        except Exception as e:
            logger.debug(f"[TradingCore] Broker issue Telegram alert failed: {e}")

    def _notify_telegram_open(self, trade: Dict) -> None:
        if not self.telegram:
            return
        try:
            # Support TelegramCommander (has method directly) and
            # TelegramManager (wraps commander in .bot) — Issue 4
            target = getattr(self.telegram, "bot", self.telegram)
            if hasattr(target, "alert_trade_opened"):
                target.alert_trade_opened(trade)
        except Exception as e:
            logger.debug(f"[TradingCore] Telegram alert failed: {e}")

    def _notify_telegram_close(self, trade: Dict) -> None:
        if not self.telegram:
            return
        try:
            target = getattr(self.telegram, "bot", self.telegram)
            if hasattr(target, "alert_trade_closed"):
                target.alert_trade_closed(trade)
        except Exception as e:
            logger.debug(f"[TradingCore] Telegram close alert failed: {e}")

    def get_asset_list(self) -> List[Tuple[str, str]]:
        return self.registry.all_assets()

    def get_strategy_stats(self) -> Dict:
        return self.state.get_all_strategy_stats()

    @staticmethod
    def _apply_gapfill_trailing_snapshot(
        current_pos: Dict[str, Any],
        *,
        management: Dict[str, Any],
        direction: str,
        entry: float,
        stop_loss: float,
        initial_risk: float,
        atr_value: float,
        trail_activation_rr: float,
        trail_atr_multiple: float,
    ) -> None:
        if not management or initial_risk <= 0.0:
            return
        if direction == "BUY":
            favorable_extreme = float(current_pos.get("highest_price", entry) or entry)
            progress_rr = (favorable_extreme - entry) / max(initial_risk, 1e-9)
        else:
            favorable_extreme = float(current_pos.get("lowest_price", entry) or entry)
            progress_rr = (entry - favorable_extreme) / max(initial_risk, 1e-9)
        if progress_rr < trail_activation_rr:
            return
        trail_dist = max(
            initial_risk * 0.85,
            atr_value * trail_atr_multiple if atr_value > 0 else 0.0,
        )
        if trail_dist <= 0.0:
            return
        current_sl = float(current_pos.get("stop_loss", stop_loss) or stop_loss)
        if direction == "BUY":
            trail_sl = favorable_extreme - trail_dist
            if trail_sl > current_sl:
                current_pos["stop_loss"] = round(trail_sl, 6)
        else:
            trail_sl = favorable_extreme + trail_dist
            if trail_sl < current_sl:
                current_pos["stop_loss"] = round(trail_sl, 6)

    @staticmethod
    def _calculate_position_pnl(
        asset: str,
        category: str,
        entry: float,
        exit_price: float,
        size: float,
        direction: str,
    ) -> float:
        try:
            from risk.position_sizer import PositionSizer as _PS

            return float(_PS.pnl(asset, category, entry, exit_price, size, direction))
        except Exception:
            return float((exit_price - entry) * size if direction == "BUY" else (entry - exit_price) * size)

    def _sync_gapfill_open_position(self, trade_id: str, current_pos: Dict[str, Any]) -> None:
        self.state.sync_open_position(current_pos)
        if self._paper_trader:
            with self._paper_trader._lock:
                self._paper_trader.open_positions[trade_id] = dict(current_pos)

    def _record_gapfill_partial_take_profit(
        self,
        *,
        current_pos: Dict[str, Any],
        trade_id: str,
        asset: str,
        category: str,
        direction: str,
        entry: float,
        tp_level: float,
        tp_idx: int,
        total_tiers: int,
        size: float,
        bar_time: Any,
        dt_open: datetime,
        df,
        break_even_after_partial: bool,
        management: Dict[str, Any],
        stop_loss: float,
        initial_risk: float,
        atr_value: float,
        trail_activation_rr: float,
        trail_atr_multiple: float,
    ) -> None:
        original_size = float(current_pos.get("position_size", size) or size)
        initial_size = float(current_pos.get("initial_position_size", size) or size)
        size_fractions = self._normalize_tp_size_fractions(
            management.get("partial_take_profit_size_fractions"),
            total_tiers,
        )
        target_fraction = float(size_fractions[tp_idx]) if tp_idx < len(size_fractions) else (1.0 / max(1, total_tiers - tp_idx))
        partial_size = min(original_size, max(0.0, initial_size * target_fraction))
        remaining_size = max(0.0, original_size - partial_size)
        partial_pnl = self._calculate_position_pnl(
            asset,
            category,
            entry,
            tp_level,
            partial_size,
            direction,
        )

        partial_trade = {
            **dict(current_pos),
            "trade_id": f"{trade_id}-PT{tp_idx + 1}",
            "parent_trade_id": trade_id,
            "is_partial_close": True,
            "position_size": partial_size,
            "lot_size": current_pos.get("lot_size"),
            "exit_price": round(tp_level, 6),
            "exit_reason": f"Partial TP {tp_idx + 1}/{total_tiers} (offline)",
            "pnl": round(partial_pnl, 6),
            "exit_time": bar_time.isoformat() if hasattr(bar_time, "isoformat") else str(bar_time),
            "duration_minutes": max(0, int((bar_time - dt_open).total_seconds() / 60)),
            "metadata": {
                **dict(current_pos.get("metadata") or {}),
                "offline_gap_fill": {
                    "breach_time": bar_time.isoformat() if hasattr(bar_time, "isoformat") else str(bar_time),
                    "checked_bars": int(len(df)),
                    "partial_tp_hit": int(tp_idx + 1),
                },
            },
        }
        current_pos["position_size"] = round(remaining_size, 8)
        current_pos["tp_hit"] = tp_idx + 1
        current_pos["management_checkpoint_at"] = (
            bar_time.isoformat() if hasattr(bar_time, "isoformat") else str(bar_time)
        )
        if break_even_after_partial:
            if direction == "BUY" and entry > float(current_pos.get("stop_loss", 0) or 0):
                current_pos["stop_loss"] = round(entry, 6)
            elif direction == "SELL" and entry < float(current_pos.get("stop_loss", 99e9) or 99e9):
                current_pos["stop_loss"] = round(entry, 6)

        self._sync_gapfill_open_position(trade_id, current_pos)
        self._record_partial_reduction(current_pos, partial_trade, partial_pnl)
        self._apply_gapfill_trailing_snapshot(
            current_pos,
            management=management,
            direction=direction,
            entry=entry,
            stop_loss=stop_loss,
            initial_risk=initial_risk,
            atr_value=atr_value,
            trail_activation_rr=trail_activation_rr,
            trail_atr_multiple=trail_atr_multiple,
        )

    def _close_gapfill_breached_position(
        self,
        *,
        pos: Dict[str, Any],
        trade_id: str,
        asset: str,
        category: str,
        direction: str,
        entry: float,
        size: float,
        breach_price: float,
        breach_reason: str,
        breach_time: Any,
        df,
    ) -> None:
        pnl = self._calculate_position_pnl(asset, category, entry, breach_price, size, direction)
        close_updates = {
            "highest_price": max(
                float(pos.get("highest_price", entry) or entry),
                float(df["high"].max()),
            ),
            "lowest_price": min(
                float(pos.get("lowest_price", entry) or entry),
                float(df["low"].min()),
            ),
            "metadata": {
                **dict(pos.get("metadata") or {}),
                "offline_gap_fill": {
                    "breach_time": breach_time.isoformat() if hasattr(breach_time, "isoformat") else str(breach_time),
                    "checked_bars": int(len(df)),
                },
            },
        }
        closed = self.state.close_position(
            trade_id,
            breach_price,
            breach_reason,
            pnl,
            extra_updates=close_updates,
        )
        if not closed:
            return
        if self._paper_trader:
            with self._paper_trader._lock:
                self._paper_trader.open_positions.pop(trade_id, None)

        self._record_trade_close_side_effects(closed, pnl)
        self._set_post_close_cooldown(asset, closed)
        logger.info(
            f"[GapFill] {asset} {direction} closed offline — "
            f"{breach_reason} @ {breach_price:.5f}  "
            f"PnL=${pnl:.2f}  breached at {breach_time}"
        )

    @staticmethod
    def _parse_gapfill_open_time(open_time: str, asset: str) -> Optional[datetime]:
        try:
            dt_open = datetime.fromisoformat(open_time)
            if dt_open.tzinfo is None:
                dt_open = dt_open.replace(tzinfo=timezone.utc)
            return dt_open
        except Exception:
            logger.debug(f"[GapFill] Cannot parse open_time for {asset} — skipping")
            return None

    @classmethod
    def _resolve_gapfill_replay_start(
        cls,
        pos: Dict[str, Any],
        *,
        asset: str,
        dt_trade_open: datetime,
    ) -> datetime:
        checkpoint = str(
            pos.get("management_checkpoint_at")
            or pos.get("last_runtime_update_at")
            or ""
        ).strip()
        if not checkpoint:
            return dt_trade_open

        dt_checkpoint = cls._parse_gapfill_open_time(checkpoint, asset)
        if dt_checkpoint is None:
            return dt_trade_open
        if dt_checkpoint < dt_trade_open:
            return dt_trade_open
        return dt_checkpoint

    def _load_gapfill_history(
        self,
        *,
        asset: str,
        category: str,
        dt_open: datetime,
        minutes_offline: float,
    ):
        from data.fetcher import DataFetcher

        fetcher = getattr(self, "fetcher", None) or DataFetcher()
        periods = max(24, int(minutes_offline // 5) + 12)
        df = fetcher.get_ohlcv(asset, category, interval="5m", periods=periods)
        if df is None or df.empty:
            logger.debug(f"[GapFill] No 5m data for {asset} — skipping")
            return None
        df = df[df.index > dt_open].copy()
        if df.empty:
            return None
        return df

    @staticmethod
    def _gapfill_take_profit_targets(pos: Dict[str, Any]) -> Tuple[float, List[float]]:
        take_profit = float(pos.get("take_profit", 0) or 0)
        tp_levels: List[float] = []
        for raw_level in list(pos.get("take_profit_levels", []) or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                tp_levels.append(round(level, 6))
        if tp_levels:
            take_profit = float(tp_levels[-1])
        return take_profit, tp_levels

    @staticmethod
    def _gapfill_management_runtime(
        metadata: Dict[str, Any],
        management: Dict[str, Any],
    ) -> Dict[str, Any]:
        atr_value = float(metadata.get("atr", 0.0) or 0.0)
        try:
            trail_activation_rr = max(0.5, float(management.get("trail_activation_rr", 1.0) or 1.0))
        except Exception:
            trail_activation_rr = 1.0
        try:
            trail_atr_multiple = max(0.4, float(management.get("trail_atr_multiple", 0.8) or 0.8))
        except Exception:
            trail_atr_multiple = 0.8
        return {
            "atr_value": atr_value,
            "trail_activation_rr": trail_activation_rr,
            "trail_atr_multiple": trail_atr_multiple,
            "break_even_after_partial": bool(management.get("break_even_after_partial", False)),
        }

    def _build_gapfill_runtime(self, pos: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        trade_id = str(pos.get("trade_id", "") or "")
        asset = str(pos.get("asset", "") or "")
        category = str(pos.get("category", "forex") or "forex")
        direction = str(pos.get("direction", pos.get("signal", "BUY")) or "BUY")
        entry = float(pos.get("entry_price", 0) or 0)
        stop_loss = float(pos.get("stop_loss", 0) or 0)
        take_profit, tp_levels = self._gapfill_take_profit_targets(pos)
        open_time = str(pos.get("open_time", "") or "")
        size = float(pos.get("position_size", 0) or 0)
        metadata = dict(pos.get("metadata") or {})
        management = (
            metadata.get("trade_management_plan")
            if isinstance(metadata.get("trade_management_plan"), dict)
            else {}
        )
        if not entry or not stop_loss or not asset:
            return None

        dt_open = self._parse_gapfill_open_time(open_time, asset)
        if dt_open is None:
            return None
        dt_replay_start = self._resolve_gapfill_replay_start(
            pos,
            asset=asset,
            dt_trade_open=dt_open,
        )
        minutes_offline = (datetime.now(tz=timezone.utc) - dt_replay_start).total_seconds() / 60
        if minutes_offline < 5:
            return None
        df = self._load_gapfill_history(
            asset=asset,
            category=category,
            dt_open=dt_replay_start,
            minutes_offline=minutes_offline,
        )
        if df is None:
            return None

        current_pos = dict(pos)
        current_pos["take_profit"] = take_profit
        if tp_levels:
            current_pos["take_profit_levels"] = list(tp_levels)
        current_pos["metadata"] = metadata
        return {
            "trade_id": trade_id,
            "asset": asset,
            "category": category,
            "direction": direction,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "tp_levels": tp_levels,
            "open_time": open_time,
            "size": size,
            "metadata": metadata,
            "management": management,
            "dt_open": dt_open,
            "dt_replay_start": dt_replay_start,
            "df": df,
            "current_pos": current_pos,
            "initial_risk": abs(entry - float(current_pos.get("original_sl", stop_loss) or stop_loss)),
            **self._gapfill_management_runtime(metadata, management),
        }

    @staticmethod
    def _gapfill_update_extremes(
        current_pos: Dict[str, Any],
        *,
        entry: float,
        bar_low: float,
        bar_high: float,
    ) -> None:
        current_pos["highest_price"] = max(float(current_pos.get("highest_price", entry) or entry), bar_high)
        current_pos["lowest_price"] = min(float(current_pos.get("lowest_price", entry) or entry), bar_low)

    def _record_gapfill_partial_from_runtime(
        self,
        runtime: Dict[str, Any],
        *,
        tp_level: float,
        tp_idx: int,
        bar_time: Any,
    ) -> None:
        self._record_gapfill_partial_take_profit(
            current_pos=runtime["current_pos"],
            trade_id=runtime["trade_id"],
            asset=runtime["asset"],
            category=runtime["category"],
            direction=runtime["direction"],
            entry=runtime["entry"],
            tp_level=tp_level,
            tp_idx=tp_idx,
            total_tiers=len(runtime["tp_levels"]),
            size=runtime["size"],
            bar_time=bar_time,
            dt_open=runtime["dt_open"],
            df=runtime["df"],
            break_even_after_partial=runtime["break_even_after_partial"],
            management=runtime["management"],
            stop_loss=runtime["stop_loss"],
            initial_risk=runtime["initial_risk"],
            atr_value=runtime["atr_value"],
            trail_activation_rr=runtime["trail_activation_rr"],
            trail_atr_multiple=runtime["trail_atr_multiple"],
        )

    @staticmethod
    def _gapfill_close_result(
        breach_price: float,
        breach_reason: str,
        breach_time: Any,
    ) -> Dict[str, Any]:
        return {
            "status": "close",
            "breach_price": float(breach_price),
            "breach_reason": str(breach_reason),
            "breach_time": breach_time,
        }

    def _handle_gapfill_buy_bar(
        self,
        runtime: Dict[str, Any],
        *,
        bar_time: Any,
        bar_low: float,
        bar_high: float,
    ) -> Optional[Dict[str, Any]]:
        current_stop = float(runtime["current_pos"].get("stop_loss", runtime["stop_loss"]) or runtime["stop_loss"])
        if bar_low <= current_stop:
            return self._gapfill_close_result(current_stop, "Stop Loss (offline)", bar_time)

        tp_idx = max(0, int(runtime["current_pos"].get("tp_hit", 0) or 0))
        if runtime["tp_levels"] and tp_idx < len(runtime["tp_levels"]):
            tp_level = float(runtime["tp_levels"][tp_idx])
            if bar_high < tp_level:
                return None
            if tp_idx + 1 >= len(runtime["tp_levels"]):
                return self._gapfill_close_result(tp_level, "Take Profit (offline)", bar_time)
            self._record_gapfill_partial_from_runtime(runtime, tp_level=tp_level, tp_idx=tp_idx, bar_time=bar_time)
            return {"status": "partial"}

        if runtime["take_profit"] and bar_high >= runtime["take_profit"]:
            return self._gapfill_close_result(runtime["take_profit"], "Take Profit (offline)", bar_time)
        return None

    def _handle_gapfill_sell_bar(
        self,
        runtime: Dict[str, Any],
        *,
        bar_time: Any,
        bar_low: float,
        bar_high: float,
    ) -> Optional[Dict[str, Any]]:
        current_stop = float(runtime["current_pos"].get("stop_loss", runtime["stop_loss"]) or runtime["stop_loss"])
        if bar_high >= current_stop:
            return self._gapfill_close_result(current_stop, "Stop Loss (offline)", bar_time)

        tp_idx = max(0, int(runtime["current_pos"].get("tp_hit", 0) or 0))
        if runtime["tp_levels"] and tp_idx < len(runtime["tp_levels"]):
            tp_level = float(runtime["tp_levels"][tp_idx])
            if bar_low > tp_level:
                return None
            if tp_idx + 1 >= len(runtime["tp_levels"]):
                return self._gapfill_close_result(tp_level, "Take Profit (offline)", bar_time)
            self._record_gapfill_partial_from_runtime(runtime, tp_level=tp_level, tp_idx=tp_idx, bar_time=bar_time)
            return {"status": "partial"}

        if runtime["take_profit"] and bar_low <= runtime["take_profit"]:
            return self._gapfill_close_result(runtime["take_profit"], "Take Profit (offline)", bar_time)
        return None

    def _scan_gapfill_history(self, runtime: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for bar_time, bar in runtime["df"].iterrows():
            bar_low = float(bar["low"])
            bar_high = float(bar["high"])
            self._gapfill_update_extremes(
                runtime["current_pos"],
                entry=runtime["entry"],
                bar_low=bar_low,
                bar_high=bar_high,
            )
            if runtime["direction"] == "BUY":
                action = self._handle_gapfill_buy_bar(
                    runtime,
                    bar_time=bar_time,
                    bar_low=bar_low,
                    bar_high=bar_high,
                )
            else:
                action = self._handle_gapfill_sell_bar(
                    runtime,
                    bar_time=bar_time,
                    bar_low=bar_low,
                    bar_high=bar_high,
                )
            if action and action.get("status") == "close":
                return action
            if action and action.get("status") == "partial":
                continue

            self._apply_gapfill_trailing_snapshot(
                runtime["current_pos"],
                management=runtime["management"],
                direction=runtime["direction"],
                entry=runtime["entry"],
                stop_loss=runtime["stop_loss"],
                initial_risk=runtime["initial_risk"],
                atr_value=runtime["atr_value"],
                trail_activation_rr=runtime["trail_activation_rr"],
                trail_atr_multiple=runtime["trail_atr_multiple"],
            )
        return None

    def _process_gapfill_runtime(self, runtime: Dict[str, Any]) -> None:
        breach = self._scan_gapfill_history(runtime)
        if breach is None:
            if runtime["df"] is not None and not runtime["df"].empty:
                last_bar_time = runtime["df"].index[-1]
                runtime["current_pos"]["management_checkpoint_at"] = (
                    last_bar_time.isoformat() if hasattr(last_bar_time, "isoformat") else str(last_bar_time)
                )
            self._sync_gapfill_open_position(runtime["trade_id"], runtime["current_pos"])
            logger.debug(f"[GapFill] {runtime['asset']}: no breach found — position remains open")
            return

        self._close_gapfill_breached_position(
            pos=runtime["current_pos"],
            trade_id=runtime["trade_id"],
            asset=runtime["asset"],
            category=runtime["category"],
            direction=runtime["direction"],
            entry=runtime["entry"],
            size=runtime["size"],
            breach_price=float(breach["breach_price"]),
            breach_reason=str(breach["breach_reason"]),
            breach_time=breach["breach_time"],
            df=runtime["df"],
        )

    def _check_offline_sl_tp(self) -> None:
        """
        Runs once on startup after positions are restored.
        For each open position, fetches 5m OHLCV bars from open_time to now
        and checks if SL or TP was breached while the bot was offline.
        If breached, closes the position at the breach price so P&L and
        trade history are accurate.
        """
        positions = self.state.get_open_positions()
        if not positions:
            return

        logger.info(f"[TradingCore] Offline gap-fill: checking {len(positions)} position(s)")

        for pos in positions:
            asset = str(pos.get("asset", "") or "")
            try:
                runtime = self._build_gapfill_runtime(pos)
                if runtime is None:
                    continue
                self._process_gapfill_runtime(runtime)
            except Exception as e:
                logger.error(f"[GapFill] {asset} gap-fill error: {e}")

    @staticmethod
    def _manual_close_exit_updates(pos: Dict[str, Any], entry: float, exit_price: float) -> Dict[str, Any]:
        return {
            "highest_price": max(float(pos.get("highest_price", entry) or entry), float(exit_price)),
            "lowest_price": min(float(pos.get("lowest_price", entry) or entry), float(exit_price)),
            "metadata": dict(pos.get("metadata") or {}),
        }

    @staticmethod
    def _manual_close_pnl(
        pos: Dict[str, Any],
        entry: float,
        exit_price: float,
        size: float,
        direction: str,
    ) -> float:
        try:
            from risk.position_sizer import PositionSizer as _PS

            return float(
                _PS.pnl(
                    pos.get("asset", ""),
                    pos.get("category", "forex"),
                    entry,
                    exit_price,
                    size,
                    direction,
                )
            )
        except Exception:
            return float((exit_price - entry) * size if direction == "BUY" else (entry - exit_price) * size)

    def _manual_close_exit_price(
        self,
        pos: Dict[str, Any],
        entry: float,
        size: float,
        direction: str,
    ) -> Tuple[float, float]:
        exit_price = entry
        pnl = 0.0
        if not self.fetcher:
            return exit_price, pnl
        try:
            price, _ = self.fetcher.get_real_time_price(
                pos.get("asset", ""), pos.get("category", "forex")
            )
            if price:
                exit_price = float(price)
                pnl = self._manual_close_pnl(pos, entry, exit_price, size, direction)
        except Exception:
            pass
        return exit_price, pnl

    def _finalize_manual_close(
        self,
        closed: Dict[str, Any],
        pnl: float,
        trade_id: str,
        *,
        reason: str = "Manual Close",
    ) -> None:
        if self._paper_trader:
            with self._paper_trader._lock:
                self._paper_trader.open_positions.pop(trade_id, None)

        try:
            canonical = self.registry.canonical(closed.get("asset", ""))
            cooldown_minutes = self._resolve_post_close_cooldown_minutes(closed)
            self.state.set_cooldown(canonical, cooldown_minutes)
            logger.info(
                f"[TradingCore] {reason} — set cooldown {cooldown_minutes}m "
                f"for {canonical}"
            )
        except Exception as e:
            logger.debug(f"[TradingCore] Manual close cooldown error: {e}")

        self._notify_telegram_close(closed)

        try:
            from services.personality_service import personality as _personality
            _personality.record_trade(closed)
        except Exception:
            pass
        try:
            from monitoring.system_health_service import monitor as _mon
            _mon.record_trade_result(pnl)
        except Exception:
            pass

        logger.log_trade(
            "CLOSE",
            trade_id=trade_id,
            asset=closed.get("asset", ""),
            pnl=round(pnl, 4),
            reason=reason,
        )

    def close_position_manually(self, trade_id: str, *, reason: str = "Manual Close") -> Optional[Dict]:
        pos = self.state.get_open_position(trade_id)
        if not pos:
            return None
        entry = float(pos.get("entry_price", 0))
        direction = pos.get("direction", pos.get("signal", "BUY"))
        size = float(pos.get("position_size", 0))
        exit_price, pnl = self._manual_close_exit_price(pos, entry, size, direction)
        broker_name = str(pos.get("broker") or pos.get("execution_mode") or "").lower()
        broker_result = None
        if broker_name.startswith("ig") or broker_name == "ig":
            router = getattr(self, "exchange_router", None)
            if router is None or not hasattr(router, "close_position"):
                logger.error(f"[TradingCore] Cannot close IG position {trade_id}: exchange router unavailable")
                return None
            broker_result = router.close_position(pos, reason=reason)
            if not broker_result or broker_result.status != "FILLED":
                logger.error(
                    f"[TradingCore] Broker close failed {trade_id}: "
                    f"{getattr(broker_result, 'error', 'no result')}"
                )
                return None
            if broker_result.avg_price and broker_result.avg_price > 0:
                exit_price = float(broker_result.avg_price)
                pnl = self._manual_close_pnl(pos, entry, exit_price, size, direction)

        close_updates = self._manual_close_exit_updates(pos, entry, exit_price)
        if broker_result is not None:
            broker_close_meta = {
                "broker": str(pos.get("broker") or "ig"),
                "status": str(getattr(broker_result, "status", "") or ""),
                "order_id": str(getattr(broker_result, "order_id", "") or ""),
                "avg_price": float(getattr(broker_result, "avg_price", 0.0) or 0.0),
                "filled_qty": float(getattr(broker_result, "filled_qty", 0.0) or 0.0),
                "reason": reason,
            }
            raw = getattr(broker_result, "raw", None)
            if isinstance(raw, dict):
                request = raw.get("request") if isinstance(raw.get("request"), dict) else {}
                confirm = raw.get("confirm") if isinstance(raw.get("confirm"), dict) else {}
                if request.get("epic"):
                    broker_close_meta["epic"] = request.get("epic")
                if confirm.get("dealStatus"):
                    broker_close_meta["deal_status"] = confirm.get("dealStatus")
                if confirm.get("reason"):
                    broker_close_meta["confirm_reason"] = confirm.get("reason")
                if confirm.get("dealReference") or request.get("dealReference"):
                    broker_close_meta["deal_reference"] = confirm.get("dealReference") or request.get("dealReference")
            close_meta = dict(close_updates.get("metadata") or {})
            close_meta["broker_close"] = broker_close_meta
            close_updates["metadata"] = close_meta
            close_updates["broker_close_status"] = broker_close_meta.get("status", "")
            close_updates["broker_close_order_id"] = broker_close_meta.get("order_id", "")
            close_updates["broker_close_avg_price"] = broker_close_meta.get("avg_price", 0.0)

        closed = self.state.close_position(
            trade_id,
            exit_price,
            reason,
            pnl,
            extra_updates=close_updates,
        )
        if not closed:
            return None

        self._finalize_manual_close(closed, pnl, trade_id, reason=reason)
        return closed

    def __repr__(self) -> str:
        return (
            f"TradingCore(mode={self.strategy_mode}, "
            f"balance={self.state.balance:.2f}, "
            f"running={self._is_running}, "
            f"positions={self.state.open_position_count()})"
        )


# ── Module-level singleton reference ──────────────────────────────────────────
# Set by bot.py after engine.start() so the signal reporter and other modules
# can access the live engine instance without circular imports.
_CORE_INSTANCE: Optional["TradingCore"] = None
