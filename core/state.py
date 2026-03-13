"""
core/state.py — SystemState: the single source of truth for all mutable state.

Every subsystem reads and writes state through this object.
No subsystem keeps its own copy of positions, balance, or cooldowns.

Persistence:
  Full state is saved to data/system_state.json on every change that
  matters (trade open/close, balance change, cooldown change).
  On startup, state is restored so restarts are seamless.

Thread safety:
  All public methods acquire self._lock before reading or writing.
  Never call one public method from inside another (deadlock risk).
  Internal _-prefixed methods assume the lock is already held.

What is persisted:
  • open_positions       (restored on restart, SL/TP resumes immediately)
  • closed_positions     (last 500 for performance calc)
  • balance              (current account balance)
  • daily_trades         (resets at midnight)
  • daily_pnl            (resets at midnight)
  • cooldowns            (asset → expiry timestamp)
  • strategy_stats       (per-strategy win/loss counts)
  • session_stats        (per-session win/loss counts)
  • last_save_date       (to detect midnight rollover)
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional

from logger import logger


_STATE_FILE = Path("data/system_state.json")
_STATE_FILE.parent.mkdir(exist_ok=True)


class SystemState:
    """
    Single source of truth for all mutable trading state.
    Thread-safe. File-persisted. Restart-safe.
    """

    def __init__(self):
        self._lock = threading.RLock()

        # ── Positions ─────────────────────────────────────────────────────
        self._open_positions: Dict[str, Dict] = {}    # trade_id → position dict
        self._closed_positions: List[Dict] = []       # last 500

        # ── Account ───────────────────────────────────────────────────────
        self._balance: float = 30.0
        self._initial_balance: float = 30.0

        # ── Daily counters (reset at midnight) ────────────────────────────
        self._daily_trades: int = 0
        self._daily_pnl: float = 0.0
        self._last_save_date: str = date.today().isoformat()

        # ── Cooldowns: canonical_asset → expiry datetime ───────────────────
        self._cooldowns: Dict[str, datetime] = {}

        # ── Strategy stats ────────────────────────────────────────────────
        # strategy_id → {'wins': int, 'losses': int, 'pnl': float}
        self._strategy_stats: Dict[str, Dict] = defaultdict(
            lambda: {"wins": 0, "losses": 0, "pnl": 0.0}
        )

        # ── Session stats ─────────────────────────────────────────────────
        # session_name → {'wins': int, 'losses': int, 'pnl': float}
        self._session_stats: Dict[str, Dict] = defaultdict(
            lambda: {"wins": 0, "losses": 0, "pnl": 0.0}
        )

        # ── Asset stats (for learned bias) ────────────────────────────────
        # canonical_asset → {'wins': int, 'losses': int, 'pnl': float}
        self._asset_stats: Dict[str, Dict] = defaultdict(
            lambda: {"wins": 0, "losses": 0, "pnl": 0.0}
        )

        # Load persisted state
        self._load()

    # ─────────────────────────────────────────────────────────────────────────
    # Positions
    # ─────────────────────────────────────────────────────────────────────────

    def add_position(self, position: Dict) -> None:
        """Record a newly opened position."""
        with self._lock:
            trade_id = position["trade_id"]
            self._open_positions[trade_id] = position
            self._daily_trades += 1
            self._persist()

    def close_position(
        self, trade_id: str, exit_price: float, exit_reason: str, pnl: float
    ) -> Optional[Dict]:
        """
        Move a position from open→closed, update balance and stats.
        Returns the closed position dict or None if not found.
        """
        with self._lock:
            pos = self._open_positions.pop(trade_id, None)
            if pos is None:
                return None

            pos["exit_price"]  = exit_price
            pos["exit_reason"] = exit_reason
            pos["pnl"]         = pnl
            pos["exit_time"]   = datetime.now().isoformat()

            # Update daily P&L
            self._daily_pnl += pnl

            # Update balance
            self._balance += pnl

            # Update stats
            sid     = pos.get("strategy_id", "UNKNOWN")
            session = pos.get("session", "unknown")
            asset   = pos.get("canonical_asset", pos.get("asset", "UNKNOWN"))

            for stats_dict, key in [
                (self._strategy_stats, sid),
                (self._session_stats,  session),
                (self._asset_stats,    asset),
            ]:
                s = stats_dict[key]
                s["pnl"] += pnl
                if pnl > 0:
                    s["wins"] += 1
                else:
                    s["losses"] += 1

            # Keep last 500 closed positions
            self._closed_positions.append(pos)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-500:]

            self._persist()
            return pos

    def get_open_positions(self) -> List[Dict]:
        """Return a snapshot of all open positions."""
        with self._lock:
            return list(self._open_positions.values())

    def get_open_position(self, trade_id: str) -> Optional[Dict]:
        with self._lock:
            return self._open_positions.get(trade_id)

    def get_closed_positions(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            return list(self._closed_positions[-limit:])

    def open_position_count(self) -> int:
        with self._lock:
            return len(self._open_positions)

    def has_open_position_for(self, canonical_asset: str) -> bool:
        """True if any open position's canonical_asset matches."""
        with self._lock:
            return any(pos.get("canonical_asset") == canonical_asset for pos in self._open_positions.values())

    def update_position_field(self, trade_id: str, **kwargs) -> None:
        """Update arbitrary fields on an open position (e.g. trailing stop)."""
        with self._lock:
            if trade_id in self._open_positions:
                self._open_positions[trade_id].update(kwargs)
                # Don't persist on every price tick — only on structural changes

    # ─────────────────────────────────────────────────────────────────────────
    # Balance
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def balance(self) -> float:
        with self._lock:
            return self._balance

    def set_balance(self, balance: float, reason: str = "init") -> None:
        with self._lock:
            self._balance         = balance
            self._initial_balance = balance
            self._persist()

    def adjust_balance(self, delta: float) -> float:
        """Add delta to balance. Returns new balance."""
        with self._lock:
            self._balance += delta
            self._persist()
            return self._balance

    # ─────────────────────────────────────────────────────────────────────────
    # Daily counters
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def daily_trades(self) -> int:
        with self._lock:
            return self._daily_trades

    @property
    def daily_pnl(self) -> float:
        with self._lock:
            return self._daily_pnl

    def check_day_rollover(self) -> bool:
        """
        Call once per trading loop cycle.
        Returns True if a new day started and counters were reset.
        """
        with self._lock:
            today = date.today().isoformat()
            if today != self._last_save_date:
                logger.info(f"[SystemState] New trading day — resetting daily counters")
                self._daily_trades    = 0
                self._daily_pnl       = 0.0
                self._last_save_date  = today
                # Expire stale cooldowns
                self._purge_expired_cooldowns()
                self._persist()
                return True
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Cooldowns
    # ─────────────────────────────────────────────────────────────────────────

    def set_cooldown(self, canonical_asset: str, minutes: int) -> None:
        """Block re-entry for this asset for `minutes` minutes."""
        from datetime import timedelta
        with self._lock:
            expiry = datetime.now() + timedelta(minutes=minutes)
            self._cooldowns[canonical_asset] = expiry
            logger.info(
                f"[SystemState] Cooldown set: {canonical_asset} for {minutes}min"
            )
            self._persist()

    def is_cooling_down(self, canonical_asset: str) -> bool:
        """True if the asset is currently in cooldown."""
        with self._lock:
            expiry = self._cooldowns.get(canonical_asset)
            if expiry is None:
                return False
            if datetime.now() >= expiry:
                del self._cooldowns[canonical_asset]
                self._persist()
                return False
            return True

    def cooldown_remaining(self, canonical_asset: str) -> int:
        """Minutes remaining in cooldown, or 0."""
        with self._lock:
            expiry = self._cooldowns.get(canonical_asset)
            if expiry is None:
                return 0
            remaining = (expiry - datetime.now()).total_seconds() / 60
            return max(0, int(remaining))

    def _purge_expired_cooldowns(self) -> None:
        """Remove expired entries (call with lock held)."""
        now = datetime.now()
        self._cooldowns = {
            k: v for k, v in self._cooldowns.items() if v > now
        }

    def get_all_cooldowns(self) -> Dict[str, int]:
        """Return {canonical_asset: remaining_minutes} for all active cooldowns."""
        with self._lock:
            self._purge_expired_cooldowns()
            return {
                k: max(0, int((v - datetime.now()).total_seconds() / 60))
                for k, v in self._cooldowns.items()
            }

    # ─────────────────────────────────────────────────────────────────────────
    # Strategy / Session / Asset stats
    # ─────────────────────────────────────────────────────────────────────────

    def get_strategy_stats(self, strategy_id: str) -> Dict:
        with self._lock:
            s = self._strategy_stats[strategy_id]
            total = s["wins"] + s["losses"]
            return {
                **s,
                "win_rate": s["wins"] / total if total else 0.0,
                "total": total,
            }

    def get_all_strategy_stats(self) -> Dict[str, Dict]:
        with self._lock:
            result = {}
            for sid, s in self._strategy_stats.items():
                total = s["wins"] + s["losses"]
                result[sid] = {
                    **s,
                    "win_rate": s["wins"] / total if total else 0.0,
                    "total": total,
                }
            return result

    def get_asset_win_rate(self, canonical_asset: str) -> float:
        with self._lock:
            s = self._asset_stats[canonical_asset]
            total = s["wins"] + s["losses"]
            return s["wins"] / total if total else 0.5

    # ─────────────────────────────────────────────────────────────────────────
    # Performance summary
    # ─────────────────────────────────────────────────────────────────────────

    def get_performance(self) -> Dict:
        """Compute and return a full performance snapshot."""
        with self._lock:
            closed = self._closed_positions
            total  = len(closed)
            wins   = sum(1 for t in closed if t.get("pnl", 0) > 0)
            losses = total - wins

            total_pnl  = sum(t.get("pnl", 0) for t in closed)
            win_pnls   = [t["pnl"] for t in closed if t.get("pnl", 0) > 0]
            loss_pnls  = [abs(t["pnl"]) for t in closed if t.get("pnl", 0) <= 0]

            avg_win   = sum(win_pnls)  / len(win_pnls)  if win_pnls  else 0.0
            avg_loss  = sum(loss_pnls) / len(loss_pnls) if loss_pnls else 0.0
            pf_num    = sum(win_pnls)
            pf_den    = sum(loss_pnls)
            pf        = pf_num / pf_den if pf_den > 0 else float("inf")

            return {
                "total_trades":    total,
                "winning_trades":  wins,
                "losing_trades":   losses,
                "win_rate":        round(wins / total * 100, 2) if total else 0.0,
                "total_pnl":       round(total_pnl, 2),
                "avg_win":         round(avg_win, 2),
                "avg_loss":        round(avg_loss, 2),
                "profit_factor":   round(pf, 3),
                "balance":         self._balance,
                "initial_balance": self._initial_balance,
                "open_positions":  len(self._open_positions),
                "daily_trades":    self._daily_trades,
                "daily_pnl":       round(self._daily_pnl, 2),
            }

    # ─────────────────────────────────────────────────────────────────────────
    # Persistence
    # ─────────────────────────────────────────────────────────────────────────

    def _to_dict(self) -> Dict:
        """Serialise full state (lock must be held)."""
        return {
            "schema_version":   2,
            "saved_at":         datetime.now().isoformat(),
            "balance":          self._balance,
            "initial_balance":  self._initial_balance,
            "daily_trades":     self._daily_trades,
            "daily_pnl":        self._daily_pnl,
            "last_save_date":   self._last_save_date,
            "open_positions":   list(self._open_positions.values()),
            "closed_positions": self._closed_positions[-500:],
            "cooldowns": {
                k: v.isoformat()
                for k, v in self._cooldowns.items()
                if v > datetime.now()
            },
            "strategy_stats": dict(self._strategy_stats),
            "session_stats":  dict(self._session_stats),
            "asset_stats":    dict(self._asset_stats),
        }

    def _persist(self) -> None:
        """Atomic write to disk (lock must be held)."""
        try:
            data = self._to_dict()
            tmp_fd, tmp_path = tempfile.mkstemp(
                prefix="system_state_", suffix=".tmp",
                dir=_STATE_FILE.parent,
            )
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp_path, _STATE_FILE)
        except Exception as e:
            logger.error(f"[SystemState] Persist failed: {e}")

    def _load(self) -> None:
        """Load state from disk. Silent on missing/corrupt file."""
        if not _STATE_FILE.exists():
            return
        try:
            raw = json.loads(_STATE_FILE.read_text(encoding="utf-8"))

            self._balance         = float(raw.get("balance",         self._balance))
            self._initial_balance = float(raw.get("initial_balance", self._balance))
            self._daily_trades    = int(raw.get("daily_trades",   0))
            self._daily_pnl       = float(raw.get("daily_pnl",    0.0))
            self._last_save_date  = raw.get("last_save_date", date.today().isoformat())

            # Restore open positions
            for pos in raw.get("open_positions", []):
                tid = pos.get("trade_id")
                if tid:
                    self._open_positions[tid] = pos

            # Restore closed positions
            self._closed_positions = raw.get("closed_positions", [])

            # Restore cooldowns (skip expired)
            now = datetime.now()
            for k, v in raw.get("cooldowns", {}).items():
                try:
                    expiry = datetime.fromisoformat(v)
                    if expiry > now:
                        self._cooldowns[k] = expiry
                except Exception:
                    pass

            # Restore stats
            for sid, s in raw.get("strategy_stats", {}).items():
                self._strategy_stats[sid].update(s)
            for sess, s in raw.get("session_stats", {}).items():
                self._session_stats[sess].update(s)
            for asset, s in raw.get("asset_stats", {}).items():
                self._asset_stats[asset].update(s)

            # Day rollover check
            if self._last_save_date != date.today().isoformat():
                self._daily_trades   = 0
                self._daily_pnl      = 0.0
                self._last_save_date = date.today().isoformat()

            n_open   = len(self._open_positions)
            n_closed = len(self._closed_positions)
            n_cool   = len(self._cooldowns)
            logger.info(
                f"[SystemState] Restored: {n_open} open positions, "
                f"{n_closed} closed trades, {n_cool} active cooldowns"
            )

        except Exception as e:
            logger.error(f"[SystemState] Load failed: {e} — starting fresh")

    def force_save(self) -> None:
        """Public method to trigger an immediate persist."""
        with self._lock:
            self._persist()

    def snapshot(self) -> Dict:
        """Return a read-only dict of the full current state (for dashboard)."""
        with self._lock:
            return self._to_dict()


# ── Global singleton ──────────────────────────────────────────────────────────
state: SystemState = SystemState()