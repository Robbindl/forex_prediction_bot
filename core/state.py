from __future__ import annotations

import json
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()

_STATE_FILE = Path("data/system_state.json")
_STATE_FILE.parent.mkdir(exist_ok=True)


def _coerce_trade_time(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _normalize_closed_trade_snapshot(trade: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(trade or {})

    entry_time = normalized.get("entry_time") or normalized.get("open_time")
    if entry_time not in (None, ""):
        normalized["entry_time"] = entry_time
        normalized.setdefault("open_time", entry_time)

    metadata = normalized.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        normalized["metadata"] = metadata

    lot_size = normalized.get("lot_size")
    if lot_size in (None, ""):
        lot_size = metadata.get("lot_size")
    if lot_size in (None, ""):
        try:
            from risk.position_sizer import PositionSizer

            position_size = float(normalized.get("position_size") or 0.0)
            asset = str(normalized.get("asset") or "")
            category = str(normalized.get("category") or "forex")
            inferred_lot = PositionSizer.lots_from_size(asset, category, position_size)
            if inferred_lot > 0:
                lot_size = inferred_lot
        except Exception:
            lot_size = None
    if lot_size not in (None, ""):
        try:
            normalized["lot_size"] = float(lot_size)
        except Exception:
            pass

    direction = (
        normalized.get("direction")
        or normalized.get("signal")
        or metadata.get("playbook_direction")
        or metadata.get("direction")
        or "BUY"
    )
    direction = str(direction or "BUY").upper()
    normalized["direction"] = direction
    normalized["signal"] = direction

    duration_raw = normalized.get("duration_minutes")
    if duration_raw in (None, ""):
        entry_dt = _coerce_trade_time(normalized.get("entry_time"))
        exit_dt = _coerce_trade_time(normalized.get("exit_time"))
        if entry_dt and exit_dt:
            normalized["duration_minutes"] = max(0, int((exit_dt - entry_dt).total_seconds() / 60))
    else:
        try:
            normalized["duration_minutes"] = max(0, int(float(duration_raw)))
        except Exception:
            normalized["duration_minutes"] = 0

    return normalized


def _closed_trade_sort_key(trade: Dict[str, Any]) -> datetime:
    return (
        _coerce_trade_time(trade.get("exit_time"))
        or _coerce_trade_time(trade.get("entry_time"))
        or _coerce_trade_time(trade.get("open_time"))
        or datetime.min.replace(tzinfo=timezone.utc)
    )


def _infer_partial_trade_shape(trade: Dict[str, Any]) -> tuple[Optional[str], bool]:
    normalized = _normalize_closed_trade_snapshot(trade)
    metadata = normalized.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    parent_trade_id = metadata.get("parent_trade_id")
    raw_id = str(normalized.get("trade_id", "") or "")
    if parent_trade_id in (None, "") and "-PT" in raw_id:
        candidate, suffix = raw_id.rsplit("-PT", 1)
        if candidate and suffix.isdigit():
            parent_trade_id = candidate
    partial_flag = metadata.get("is_partial_close")
    if partial_flag is None:
        partial_flag = bool(parent_trade_id) or str(normalized.get("exit_reason") or "").lower().startswith("partial tp")
    clean_parent = str(parent_trade_id) if parent_trade_id not in (None, "") else None
    return clean_parent, bool(partial_flag)


def rollup_closed_trade_history(trades: List[Dict[str, Any]], limit: int = 100) -> List[Dict[str, Any]]:
    normalized = [_normalize_closed_trade_snapshot(t) for t in trades or []]
    partials_by_parent: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    parents: List[Dict[str, Any]] = []

    for trade in normalized:
        parent_trade_id, is_partial_close = _infer_partial_trade_shape(trade)
        trade["parent_trade_id"] = parent_trade_id
        trade["is_partial_close"] = is_partial_close
        if is_partial_close and parent_trade_id:
            partials_by_parent[parent_trade_id].append(trade)
        else:
            parents.append(trade)

    rolled: List[Dict[str, Any]] = []
    for trade in parents:
        trade_id = str(trade.get("trade_id", "") or "")
        partials = sorted(partials_by_parent.get(trade_id, []), key=_closed_trade_sort_key)
        partial_pnl = sum(float(p.get("pnl") or 0.0) for p in partials)
        runner_pnl = float(trade.get("pnl") or 0.0)
        total_pnl = runner_pnl + partial_pnl
        final_reason = str(trade.get("exit_reason", "") or "")

        row = dict(trade)
        row["partial_close_count"] = len(partials)
        row["has_partial_closes"] = bool(partials)
        row["partial_realized_pnl"] = round(partial_pnl, 8)
        row["runner_realized_pnl"] = round(runner_pnl, 8)
        row["total_realized_pnl"] = round(total_pnl, 8)
        row["pnl"] = round(total_pnl, 8)
        row["partial_trade_ids"] = [str(p.get("trade_id", "") or "") for p in partials]
        row["partial_exit_reasons"] = [str(p.get("exit_reason", "") or "") for p in partials]
        row["continued_after_partial"] = bool(partials)
        if partials:
            partial_label = f"Partial TP x{len(partials)}"
            row["display_exit_reason"] = f"{partial_label} -> {final_reason or 'Runner closed'}"
            row["continuation_summary"] = f"{partial_label} | Runner {runner_pnl:+.2f} | Total {total_pnl:+.2f}"
        else:
            row["display_exit_reason"] = final_reason
            row["continuation_summary"] = ""
        rolled.append(row)

    rolled = sorted(rolled, key=_closed_trade_sort_key, reverse=True)
    return rolled[:limit]


def _merge_trade_metadata(base: Any, extra: Any) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(base, dict):
        merged.update(base)
    if isinstance(extra, dict):
        merged.update(extra)
    return merged


def _attach_execution_feedback(snapshot: Dict[str, Any]) -> None:
    try:
        from services.execution_feedback_service import get_service as get_execution_feedback_service

        feedback = get_execution_feedback_service().analyze_trade(snapshot)
        if isinstance(feedback, dict) and feedback:
            metadata = _merge_trade_metadata(snapshot.get("metadata"), None)
            metadata["execution_feedback"] = feedback
            try:
                from services.post_trade_review_service import get_service as get_post_trade_review_service

                review = get_post_trade_review_service().build_review(
                    {
                        **snapshot,
                        "metadata": metadata,
                    }
                )
                if isinstance(review, dict) and review:
                    metadata["post_trade_review"] = review
            except Exception as review_exc:
                logger.debug(f"[State] Post-trade review attach skipped: {review_exc}")
            snapshot["metadata"] = metadata
    except Exception as e:
        logger.debug(f"[State] Execution feedback attach skipped: {e}")


class SystemState:
    """Single source of truth. Thread-safe. DB-persisted + JSON cache."""

    def __init__(self):
        self._lock = threading.RLock()

        # ── In-memory state ───────────────────────────────────────────────
        self._open_positions:   Dict[str, Dict] = {}
        self._closed_positions: List[Dict]      = []
        self._balance:          float = 10000.0  # FIX: Changed from $30 to realistic trading account
        self._initial_balance:  float = 10000.0
        self._daily_trades:     int   = 0
        self._daily_pnl:        float = 0.0
        self._last_save_date:   str   = date.today().isoformat()
        self._cooldowns:        Dict[str, datetime] = {}
        self._strategy_stats:   Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._session_stats:    Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._asset_stats:      Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._last_entry_time:  Optional[datetime] = None
        self._last_entry_time_loaded: bool = False

        # ── Load ──────────────────────────────────────────────────────────
        self._load_json()                # load balance / cooldowns / counters
        # DB loads delayed until init_db() called after DB ready

    def init_db(self) -> None:
        """Load DB-dependent state after DB is initialized."""
        self._load_positions_from_db()
        self._rebuild_stats_from_db()
        self._prime_last_entry_time_from_db()

    # ── Positions ─────────────────────────────────────────────────────────────

    def add_position(self, position: Dict) -> None:
        """Record a newly opened position in memory AND database."""
        with self._lock:
            trade_id = position["trade_id"]
            self._open_positions[trade_id] = position
            self._daily_trades += 1
            self._remember_entry_time_unlocked(position)
            self._persist_json()

        # Write to DB outside lock
        try:
            from services.db_pool import get_db
            get_db().save_open_position(position)
        except Exception as e:
            logger.error(f"[State] DB save_open_position failed: {e}")

    @staticmethod
    def _close_position_duration_minutes(trade_id: str, entry_time_str: Any, exit_time: str) -> int:
        open_dt = _coerce_trade_time(entry_time_str)
        exit_dt = _coerce_trade_time(exit_time)
        if not open_dt or not exit_dt:
            return 0
        try:
            duration_seconds = (exit_dt - open_dt).total_seconds()
            duration_minutes = int(duration_seconds / 60)
            return max(0, duration_minutes)
        except Exception as e:
            logger.debug(f"[State] Duration calc failed for {trade_id}: {e} — using 0")
            return 0

    @staticmethod
    def _apply_close_updates(pos: Dict[str, Any], extra_updates: Optional[Dict[str, Any]]) -> None:
        if not extra_updates:
            return
        extra = dict(extra_updates)
        update_meta = extra.pop("metadata", None)
        if update_meta is None:
            update_meta = extra.pop("trade_metadata", None)
        for key, value in extra.items():
            if value is not None:
                pos[key] = value
        if update_meta is not None:
            pos["metadata"] = _merge_trade_metadata(pos.get("metadata"), update_meta)

    def _finalize_closed_position_state(self, pos: Dict[str, Any], pnl: float) -> tuple[Dict[str, Any], float, str]:
        self._daily_pnl += pnl
        self._balance = max(0.0, self._balance + pnl)

        sid = pos.get("strategy_id", "UNKNOWN")
        session = pos.get("session", "unknown")
        asset = pos.get("canonical_asset", pos.get("asset", "UNKNOWN"))
        today = date.today().isoformat()

        for stats_dict, key in [
            (self._strategy_stats, sid),
            (self._session_stats, session),
            (self._asset_stats, asset),
        ]:
            s = stats_dict[key]
            s["pnl"] += pnl
            if pnl > 0:
                s["wins"] += 1
            else:
                s["losses"] += 1

        self._closed_positions.append(pos)
        if len(self._closed_positions) > 500:
            self._closed_positions = self._closed_positions[-500:]

        pos_snapshot = dict(pos)
        balance_now = self._balance
        self._persist_json()
        return pos_snapshot, balance_now, today

    def close_position(
        self,
        trade_id: str,
        exit_price: float,
        exit_reason: str,
        pnl: float,
        extra_updates: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict]:
        with self._lock:
            pos = self._open_positions.pop(trade_id, None)
            if pos is None:
                return None

            entry_time_str = pos.get("entry_time") or pos.get("open_time", "")
            if entry_time_str:
                pos["entry_time"] = entry_time_str
                pos.setdefault("open_time", entry_time_str)

            exit_time = datetime.now(timezone.utc).isoformat()
            duration_minutes = self._close_position_duration_minutes(trade_id, entry_time_str, exit_time)

            pos.update({
                "exit_price":       exit_price,
                "exit_reason":      exit_reason,
                "pnl":              pnl,
                "exit_time":        exit_time,
                "duration_minutes": duration_minutes,
            })
            self._apply_close_updates(pos, extra_updates)

            pos = _normalize_closed_trade_snapshot(pos)
            _attach_execution_feedback(pos)
            pos_snapshot, balance_now, today = self._finalize_closed_position_state(pos, pnl)

        # DB writes outside lock
        try:
            from services.db_pool import get_db
            db = get_db()
            db.save_trade(pos_snapshot)
            db.delete_open_position(trade_id)
            db.upsert_daily_stats(today, pnl, balance_now)
        except Exception as e:
            logger.error(f"[State] DB close_position failed: {e}")

        return pos_snapshot

    def get_open_positions(self) -> List[Dict]:
        with self._lock:
            return list(self._open_positions.values())

    def get_open_position(self, trade_id: str) -> Optional[Dict]:
        with self._lock:
            return self._open_positions.get(trade_id)

    def get_closed_positions(self, limit: int = 100) -> List[Dict]:
        """Return from memory cache; fall back to DB for older records."""
        with self._lock:
            cached = [_normalize_closed_trade_snapshot(t) for t in reversed(self._closed_positions[-limit:])]
        if len(cached) >= limit:
            return sorted(cached, key=_closed_trade_sort_key, reverse=True)[:limit]

        try:
            from services.db_pool import get_db
            db_recent = [_normalize_closed_trade_snapshot(t) for t in get_db().get_recent_trades(limit)]
        except Exception:
            return sorted(cached, key=_closed_trade_sort_key, reverse=True)[:limit]

        seen_trade_ids = {
            str(trade.get("trade_id", "") or "")
            for trade in cached
            if trade.get("trade_id")
        }
        merged = list(cached)

        for trade in db_recent:
            trade_id = str(trade.get("trade_id", "") or "")
            if trade_id and trade_id in seen_trade_ids:
                continue
            merged.append(trade)
            if trade_id:
                seen_trade_ids.add(trade_id)
            if len(merged) >= limit:
                break

        merged = sorted(
            [_normalize_closed_trade_snapshot(t) for t in merged],
            key=_closed_trade_sort_key,
            reverse=True,
        )
        return merged[:limit]

    def open_position_count(self) -> int:
        with self._lock:
            return len(self._open_positions)

    @staticmethod
    def _extract_entry_time(record: Optional[Dict[str, Any]]) -> Optional[datetime]:
        if not isinstance(record, dict):
            return None
        return _coerce_trade_time(record.get("entry_time") or record.get("open_time"))

    def _remember_entry_time_unlocked(self, record: Optional[Dict[str, Any]]) -> None:
        entry_time = self._extract_entry_time(record)
        if entry_time is None:
            return
        if self._last_entry_time is None or entry_time > self._last_entry_time:
            self._last_entry_time = entry_time
        self._last_entry_time_loaded = True

    def _prime_last_entry_time_from_db(self) -> None:
        if self._last_entry_time_loaded:
            return
        latest_entry: Optional[datetime] = None
        try:
            from services.db_pool import get_db

            recent = list(get_db().get_recent_trades(limit=25) or [])
            for trade in recent:
                entry_time = self._extract_entry_time(trade)
                if entry_time is not None and (latest_entry is None or entry_time > latest_entry):
                    latest_entry = entry_time
        except Exception as e:
            logger.debug(f"[State] last entry lookup skipped: {e}")
        with self._lock:
            if latest_entry is not None and (self._last_entry_time is None or latest_entry > self._last_entry_time):
                self._last_entry_time = latest_entry
            self._last_entry_time_loaded = True

    def get_last_entry_time(self) -> Optional[datetime]:
        with self._lock:
            cached = self._last_entry_time
            loaded = self._last_entry_time_loaded
        if cached is not None:
            return cached
        if not loaded:
            self._prime_last_entry_time_from_db()
            with self._lock:
                return self._last_entry_time
        return None

    def hours_since_last_entry(self, now: Optional[datetime] = None) -> Optional[float]:
        last_entry = self.get_last_entry_time()
        if last_entry is None:
            return None
        reference = now or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        try:
            delta_seconds = (reference.astimezone(timezone.utc) - last_entry).total_seconds()
            return max(0.0, delta_seconds / 3600.0)
        except Exception:
            return None

    def has_open_position_for(self, canonical_asset: str) -> bool:
        with self._lock:
            return any(
                p.get("canonical_asset") == canonical_asset
                for p in self._open_positions.values()
            )

    def update_position_field(self, trade_id: str, **kwargs) -> None:
        snapshot = None
        with self._lock:
            if trade_id in self._open_positions:
                self._open_positions[trade_id].update(kwargs)
                snapshot = dict(self._open_positions[trade_id])
                self._persist_json()

        if snapshot is None:
            return

        try:
            from services.db_pool import get_db
            get_db().save_open_position(snapshot)
        except Exception as e:
            logger.error(f"[State] DB update_position_field failed: {e}")

    def sync_open_position(self, position: Dict) -> None:
        """Persist the latest snapshot of an already-open position."""
        trade_id = str(position.get("trade_id", "") or "")
        if not trade_id:
            return

        snapshot = dict(position)
        with self._lock:
            self._open_positions[trade_id] = snapshot
            self._persist_json()

        try:
            from services.db_pool import get_db
            get_db().save_open_position(snapshot)
        except Exception as e:
            logger.error(f"[State] DB sync_open_position failed: {e}")

    # ── Balance ───────────────────────────────────────────────────────────────

    @property
    def balance(self) -> float:
        with self._lock:
            return self._balance

    @property
    def initial_balance(self) -> float:
        with self._lock:
            return self._initial_balance

    def set_balance(self, balance: float, reason: str = "init") -> None:
        with self._lock:
            self._balance        = balance
            self._initial_balance = balance
            self._persist_json()

    def adjust_balance(self, delta: float) -> float:
        with self._lock:
            self._balance += delta
            self._persist_json()
            return self._balance

    # ── Daily counters ────────────────────────────────────────────────────────

    @property
    def daily_trades(self) -> int:
        with self._lock:
            return self._daily_trades

    @property
    def daily_pnl(self) -> float:
        with self._lock:
            return self._daily_pnl

    def check_day_rollover(self) -> bool:
        with self._lock:
            today = date.today().isoformat()
            if today != self._last_save_date:
                logger.info("[State] New trading day — resetting daily counters")
                self._daily_trades   = 0
                self._daily_pnl      = 0.0
                self._last_save_date = today
                self._purge_expired_cooldowns()
                self._persist_json()
                return True
            return False

    # ── Cooldowns ─────────────────────────────────────────────────────────────

    def set_cooldown(self, canonical_asset: str, minutes: int) -> None:
        from datetime import timedelta
        with self._lock:
            self._cooldowns[canonical_asset] = datetime.now() + timedelta(minutes=minutes)
            self._persist_json()

    def is_cooling_down(self, canonical_asset: str) -> bool:
        with self._lock:
            expiry = self._cooldowns.get(canonical_asset)
            if expiry is None:
                return False
            if datetime.now() >= expiry:
                del self._cooldowns[canonical_asset]
                self._persist_json()
                return False
            return True

    def cooldown_remaining(self, canonical_asset: str) -> int:
        with self._lock:
            expiry = self._cooldowns.get(canonical_asset)
            if not expiry:
                return 0
            return max(0, int((expiry - datetime.now()).total_seconds() / 60))

    def get_all_cooldowns(self) -> Dict[str, int]:
        with self._lock:
            self._purge_expired_cooldowns()
            return {
                k: max(0, int((v - datetime.now()).total_seconds() / 60))
                for k, v in self._cooldowns.items()
            }

    def _purge_expired_cooldowns(self) -> None:
        now = datetime.now()
        self._cooldowns = {k: v for k, v in self._cooldowns.items() if v > now}

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_all_strategy_stats(self) -> Dict[str, Dict]:
        with self._lock:
            result = {}
            for sid, s in self._strategy_stats.items():
                total = s["wins"] + s["losses"]
                result[sid] = {**s, "win_rate": s["wins"] / total if total else 0.0, "total": total}
            return result

    def get_asset_win_rate(self, canonical_asset: str) -> float:
        with self._lock:
            s     = self._asset_stats[canonical_asset]
            total = s["wins"] + s["losses"]
            return s["wins"] / total if total else 0.5

    # ── Performance ───────────────────────────────────────────────────────────

    def get_performance(self) -> Dict:
        """Pull from DB for accuracy; fall back to memory."""
        try:
            from services.db_pool import get_db
            db_perf = get_db().get_performance_summary(days=365)
            if db_perf.get("total_trades", 0) > 0:
                with self._lock:
                    db_perf["balance"]         = self._balance
                    db_perf["initial_balance"] = self._initial_balance
                    db_perf["open_positions"]  = len(self._open_positions)
                    db_perf["daily_trades"]    = self._daily_trades
                    db_perf["daily_pnl"]       = round(self._daily_pnl, 4)
                return db_perf
        except Exception:
            pass

        # Memory fallback
        with self._lock:
            closed = self._closed_positions
            total  = len(closed)
            wins   = sum(1 for t in closed if t.get("pnl", 0) > 0)
            total_pnl = sum(t.get("pnl", 0) for t in closed)
            return {
                "total_trades":    total,
                "winning_trades":  wins,
                "losing_trades":   total - wins,
                "win_rate":        round(wins / total * 100, 2) if total else 0.0,
                "total_pnl":       round(total_pnl, 4),
                "balance":         self._balance,
                "initial_balance": self._initial_balance,
                "open_positions":  len(self._open_positions),
                "daily_trades":    self._daily_trades,
                "daily_pnl":       round(self._daily_pnl, 4),
            }

    def record_partial_close(self, parent_trade_id: str, partial_trade: Dict) -> Optional[Dict]:
        """
        Record realised PnL for a partial close while keeping the parent position open.
        """
        partial_snapshot = None
        remaining_snapshot = None
        balance_now = 0.0
        today = date.today().isoformat()

        with self._lock:
            parent = self._open_positions.get(parent_trade_id)
            if parent is None:
                return None

            partial_snapshot = _normalize_closed_trade_snapshot(dict(partial_trade))
            if not partial_snapshot.get("entry_time"):
                entry_time = parent.get("entry_time") or parent.get("open_time")
                if entry_time:
                    partial_snapshot["entry_time"] = entry_time
                    partial_snapshot.setdefault("open_time", entry_time)
            _attach_execution_feedback(partial_snapshot)
            pnl = float(partial_snapshot.get("pnl", 0.0))

            self._daily_pnl += pnl
            self._balance = max(0.0, self._balance + pnl)
            balance_now = self._balance

            sid = partial_snapshot.get("strategy_id", parent.get("strategy_id", "UNKNOWN"))
            session = partial_snapshot.get("session", parent.get("session", "unknown"))
            asset = partial_snapshot.get(
                "canonical_asset",
                parent.get("canonical_asset", parent.get("asset", "UNKNOWN")),
            )

            for stats_dict, key in [
                (self._strategy_stats, sid),
                (self._session_stats, session),
                (self._asset_stats, asset),
            ]:
                s = stats_dict[key]
                s["pnl"] += pnl
                if pnl > 0:
                    s["wins"] += 1
                else:
                    s["losses"] += 1

            self._closed_positions.append(partial_snapshot)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-500:]

            remaining_snapshot = dict(parent)
            self._persist_json()

        try:
            from services.db_pool import get_db
            db = get_db()
            db.save_trade(partial_snapshot)
            db.save_open_position(remaining_snapshot)
            db.upsert_daily_stats(
                today,
                float(partial_snapshot.get("pnl", 0.0)),
                balance_now,
                trade_count_delta=0,
            )
        except Exception as e:
            logger.error(f"[State] DB record_partial_close failed: {e}")

        return partial_snapshot

    # ── Persistence ───────────────────────────────────────────────────────────

    def _persist_json(self) -> None:
        """Write balance / cooldowns / counters to local JSON (fast cache)."""
        try:
            data = {
                "schema_version":  4,
                "saved_at":        datetime.now().isoformat(),
                "balance":         self._balance,
                "initial_balance": self._initial_balance,
                "daily_trades":    self._daily_trades,
                "daily_pnl":       self._daily_pnl,
                "last_save_date":  self._last_save_date,
                "cooldowns":       {k: v.isoformat() for k, v in self._cooldowns.items() if v > datetime.now()},
                "strategy_stats":  dict(self._strategy_stats),
                "session_stats":   dict(self._session_stats),
                "asset_stats":     dict(self._asset_stats),
                "open_positions":  list(self._open_positions.values()),
                "closed_positions": list(self._closed_positions[-200:]),
            }
            fd, tmp = tempfile.mkstemp(prefix="state_", suffix=".tmp", dir=_STATE_FILE.parent)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp, _STATE_FILE)
        except Exception as e:
            logger.error(f"[State] JSON persist failed: {e}")

    def _load_json(self) -> None:
        if not _STATE_FILE.exists():
            return
        try:
            raw = json.loads(_STATE_FILE.read_text(encoding="utf-8"))
            self._balance         = float(raw.get("balance",         self._balance))
            self._initial_balance = float(raw.get("initial_balance", self._balance))
            self._daily_trades    = int(raw.get("daily_trades",      0))
            self._daily_pnl       = float(raw.get("daily_pnl",       0.0))
            self._last_save_date  = raw.get("last_save_date", date.today().isoformat())

            now = datetime.now()
            for k, v in raw.get("cooldowns", {}).items():
                try:
                    exp = datetime.fromisoformat(v)
                    if exp > now:
                        self._cooldowns[k] = exp
                except Exception:
                    pass

            for sid, s in raw.get("strategy_stats", {}).items():
                self._strategy_stats[sid].update(s)
            for sess, s in raw.get("session_stats", {}).items():
                self._session_stats[sess].update(s)
            for asset, s in raw.get("asset_stats", {}).items():
                self._asset_stats[asset].update(s)

            # Restore open positions from JSON fallback (useful when DB is unavailable)
            open_positions = raw.get("open_positions", [])
            for pos in open_positions:
                tid = pos.get("trade_id")
                if tid and tid not in self._open_positions:
                    self._open_positions[tid] = pos
                    self._remember_entry_time_unlocked(pos)

            closed_positions = raw.get("closed_positions", [])
            if closed_positions:
                self._closed_positions = list(closed_positions)[-500:]
                for pos in self._closed_positions:
                    self._remember_entry_time_unlocked(pos)

            # Day rollover
            if self._last_save_date != date.today().isoformat():
                self._daily_trades   = 0
                self._daily_pnl      = 0.0
                self._last_save_date = date.today().isoformat()

            logger.info(f"[State] JSON loaded — balance=${self._balance:.2f}")
        except Exception as e:
            logger.error(f"[State] JSON load failed: {e} — using defaults")

    def _restore_open_positions_from_db_rows(self, positions: List[Dict[str, Any]]) -> tuple[int, List[str]]:
        restored = 0
        closed_trade_ids = {
            str(pos.get("trade_id", "") or "")
            for pos in self._closed_positions
            if pos.get("trade_id")
        }
        stale_open_trade_ids: List[str] = []

        for pos in positions:
            tid = str(pos.get("trade_id", "") or "")
            if not tid:
                continue
            if tid in closed_trade_ids:
                stale_open_trade_ids.append(tid)
                continue
            self._open_positions[tid] = pos
            self._remember_entry_time_unlocked(pos)
            restored += 1

        return restored, stale_open_trade_ids

    @staticmethod
    def _remove_stale_open_positions_from_db(db: Any, stale_open_trade_ids: List[str]) -> None:
        for trade_id in stale_open_trade_ids:
            try:
                db.delete_open_position(trade_id)
                logger.warning(
                    f"[State] Removed stale DB open position already marked closed in cache: {trade_id}"
                )
            except Exception as e:
                logger.error(f"[State] failed removing stale open position {trade_id}: {e}")

    def _sync_open_positions_to_db(self, db: Any) -> None:
        for pos in list(self._open_positions.values()):
            try:
                db.save_open_position(pos)
            except Exception:
                pass

    def _backfill_cached_open_positions_to_db(self, db: Any) -> None:
        if not self._open_positions:
            return
        logger.info(
            f"[State] Backfilling {len(self._open_positions)} cached open position(s) into DB"
        )
        for pos in list(self._open_positions.values()):
            try:
                db.save_open_position(pos)
            except Exception as e:
                logger.error(f"[State] failed backfilling open position {pos.get('trade_id')}: {e}")

    def _load_positions_from_db(self) -> None:
        """Restore open positions from PostgreSQL on startup."""
        try:
            from services.db_pool import get_db
            db = get_db()
            positions = db.load_open_positions()
            if positions:
                restored, stale_open_trade_ids = self._restore_open_positions_from_db_rows(positions)
                logger.info(f"[State] Restored {restored} open position(s) from DB")
                self._remove_stale_open_positions_from_db(db, stale_open_trade_ids)
                self._sync_open_positions_to_db(db)
            else:
                logger.info("[State] No open positions found in DB; using cached JSON state fallback if available")
                self._backfill_cached_open_positions_to_db(db)

        except Exception as e:
            logger.error(f"[State] DB position restore failed: {e}")

    def _rebuild_stats_from_db(self) -> None:
        """
        Rebuild strategy_stats and asset_stats from the trades table.
        Called on startup after _load_json() so stats survive even if
        system_state.json is deleted or corrupted.
        If _load_json() already populated the stats dicts, those values
        take precedence — this only fills in what is missing.
        """
        try:
            from services.db_pool import get_db
            db = get_db()
            rollups = db.get_closed_trade_rollups()
            rows = rollups["rows"]

            if not rows:
                return

            db_strategy = rollups["strategy"]
            db_asset = rollups["asset"]

            # Merge into in-memory dicts — DB is source of truth if JSON was empty
            with self._lock:
                json_has_strategy = any(
                    v["wins"] + v["losses"] > 0
                    for v in self._strategy_stats.values()
                )
                json_has_asset = any(
                    v["wins"] + v["losses"] > 0
                    for v in self._asset_stats.values()
                )

                if not json_has_strategy:
                    for sid, s in db_strategy.items():
                        self._strategy_stats[sid].update(s)
                    logger.info(
                        f"[State] Rebuilt strategy stats from DB "                        f"({len(db_strategy)} strategies, {len(rows)} trades)"
                    )

                if not json_has_asset:
                    for asset, s in db_asset.items():
                        self._asset_stats[asset].update(s)

        except Exception as e:
            logger.error(f"[State] _rebuild_stats_from_db failed: {e}")

    def force_save(self) -> None:
        with self._lock:
            self._persist_json()

    def snapshot(self) -> Dict:
        with self._lock:
            return {
                "balance":         self._balance,
                "daily_trades":    self._daily_trades,
                "daily_pnl":       self._daily_pnl,
                "open_positions":  list(self._open_positions.values()),
                "cooldowns":       self.get_all_cooldowns(),
            }

    def clear_trade_history(self) -> None:
        """Clear in-memory closed trade history and reset performance stats."""
        with self._lock:
            self._closed_positions = []
            self._strategy_stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
            self._session_stats  = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
            self._asset_stats    = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
            self._daily_trades = 0
            self._daily_pnl = 0.0
            self._persist_json()


state: SystemState = SystemState()
