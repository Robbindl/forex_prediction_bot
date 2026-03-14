"""
core/state.py — SystemState: single source of truth for all mutable state.

Persistence strategy:
  PRIMARY:   PostgreSQL via DatabaseService (all trades, open positions, daily stats)
  SECONDARY: data/system_state.json (fast local cache for balance/cooldowns/counters)

On startup: open positions are restored from DB (OpenPosition table).
On trade open: written to DB immediately.
On trade close: DB updated, open_position row deleted.
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

from utils.logger import get_logger

logger = get_logger()

_STATE_FILE = Path("data/system_state.json")
_STATE_FILE.parent.mkdir(exist_ok=True)


class SystemState:
    """Single source of truth. Thread-safe. DB-persisted + JSON cache."""

    def __init__(self):
        self._lock = threading.RLock()

        # ── In-memory state ───────────────────────────────────────────────
        self._open_positions:   Dict[str, Dict] = {}
        self._closed_positions: List[Dict]      = []
        self._balance:          float = 30.0
        self._initial_balance:  float = 30.0
        self._daily_trades:     int   = 0
        self._daily_pnl:        float = 0.0
        self._last_save_date:   str   = date.today().isoformat()
        self._cooldowns:        Dict[str, datetime] = {}
        self._strategy_stats:   Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._session_stats:    Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self._asset_stats:      Dict[str, Dict] = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})

        # ── Load ──────────────────────────────────────────────────────────
        self._load_json()        # load balance / cooldowns / counters
        self._load_positions_from_db()   # restore open positions from DB

    # ── Positions ─────────────────────────────────────────────────────────────

    def add_position(self, position: Dict) -> None:
        """Record a newly opened position in memory AND database."""
        with self._lock:
            trade_id = position["trade_id"]
            self._open_positions[trade_id] = position
            self._daily_trades += 1
            self._persist_json()

        # Write to DB outside lock
        try:
            from services.db_pool import get_db
            get_db().save_open_position(position)
        except Exception as e:
            logger.error(f"[State] DB save_open_position failed: {e}")

    def close_position(
        self, trade_id: str, exit_price: float, exit_reason: str, pnl: float
    ) -> Optional[Dict]:
        with self._lock:
            pos = self._open_positions.pop(trade_id, None)
            if pos is None:
                return None

            pos.update({
                "exit_price":  exit_price,
                "exit_reason": exit_reason,
                "pnl":         pnl,
                "exit_time":   datetime.utcnow().isoformat(),
            })

            self._daily_pnl  += pnl
            self._balance    += pnl

            # Stats
            sid     = pos.get("strategy_id", "UNKNOWN")
            session = pos.get("session", "unknown")
            asset   = pos.get("canonical_asset", pos.get("asset", "UNKNOWN"))
            today   = date.today().isoformat()

            for stats_dict, key in [
                (self._strategy_stats, sid),
                (self._session_stats,  session),
                (self._asset_stats,    asset),
            ]:
                s = stats_dict[key]
                s["pnl"] += pnl
                if pnl > 0:  s["wins"]   += 1
                else:        s["losses"] += 1

            self._closed_positions.append(pos)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-500:]

            pos_snapshot = dict(pos)
            balance_now  = self._balance
            self._persist_json()

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
            cached = list(self._closed_positions[-limit:])
        if len(cached) >= limit:
            return cached
        # Supplement from DB
        try:
            from services.db_pool import get_db
            return get_db().get_recent_trades(limit)
        except Exception:
            return cached

    def open_position_count(self) -> int:
        with self._lock:
            return len(self._open_positions)

    def has_open_position_for(self, canonical_asset: str) -> bool:
        with self._lock:
            return any(
                p.get("canonical_asset") == canonical_asset
                for p in self._open_positions.values()
            )

    def update_position_field(self, trade_id: str, **kwargs) -> None:
        with self._lock:
            if trade_id in self._open_positions:
                self._open_positions[trade_id].update(kwargs)

    # ── Balance ───────────────────────────────────────────────────────────────

    @property
    def balance(self) -> float:
        with self._lock:
            return self._balance

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

    # ── Persistence ───────────────────────────────────────────────────────────

    def _persist_json(self) -> None:
        """Write balance / cooldowns / counters to local JSON (fast cache)."""
        try:
            data = {
                "schema_version":  3,
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

            # Day rollover
            if self._last_save_date != date.today().isoformat():
                self._daily_trades   = 0
                self._daily_pnl      = 0.0
                self._last_save_date = date.today().isoformat()

            logger.info(f"[State] JSON loaded — balance=${self._balance:.2f}")
        except Exception as e:
            logger.error(f"[State] JSON load failed: {e} — using defaults")

    def _load_positions_from_db(self) -> None:
        """Restore open positions from PostgreSQL on startup."""
        try:
            from services.db_pool import get_db
            positions = get_db().load_open_positions()
            for pos in positions:
                tid = pos.get("trade_id")
                if tid:
                    self._open_positions[tid] = pos
            if positions:
                logger.info(f"[State] Restored {len(positions)} open position(s) from DB")
        except Exception as e:
            logger.error(f"[State] DB position restore failed: {e}")

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


state: SystemState = SystemState()