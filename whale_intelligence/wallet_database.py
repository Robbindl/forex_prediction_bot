from __future__ import annotations

import json
import time
from typing import Dict, List, Optional, TYPE_CHECKING

from sqlalchemy import text
from utils.logger import get_logger

if TYPE_CHECKING:
    from whale_intelligence.wallet_behavior_classifier import WalletProfile

logger = get_logger()

# ── SQL definitions ───────────────────────────────────────────────────────────

_CREATE_WALLETS = """
CREATE TABLE IF NOT EXISTS whale_wallets (
    address      TEXT PRIMARY KEY,
    label        TEXT        NOT NULL DEFAULT '',
    chain        TEXT        NOT NULL DEFAULT 'btc',
    wallet_type  TEXT        NOT NULL DEFAULT 'unknown',
    created_at   BIGINT      NOT NULL DEFAULT 0
);
"""

_CREATE_BALANCES = """
CREATE TABLE IF NOT EXISTS whale_balances (
    address      TEXT PRIMARY KEY,
    balance      DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at   BIGINT NOT NULL DEFAULT 0
);
"""

_CREATE_MOVEMENTS = """
CREATE TABLE IF NOT EXISTS whale_movements (
    id           SERIAL PRIMARY KEY,
    address      TEXT        NOT NULL,
    delta        DOUBLE PRECISION NOT NULL,
    balance_after DOUBLE PRECISION NOT NULL,
    asset        TEXT        NOT NULL DEFAULT 'BTC',
    event_type   TEXT        NOT NULL DEFAULT '',
    ts           BIGINT      NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_whale_movements_address ON whale_movements(address);
CREATE INDEX IF NOT EXISTS idx_whale_movements_ts      ON whale_movements(ts);
"""

_CREATE_PROFILES = """
CREATE TABLE IF NOT EXISTS whale_profiles (
    address      TEXT PRIMARY KEY,
    behavior     TEXT        NOT NULL DEFAULT 'unknown',
    confidence   REAL        NOT NULL DEFAULT 0,
    history_json TEXT        NOT NULL DEFAULT '[]',
    last_active  BIGINT      NOT NULL DEFAULT 0,
    updated_at   BIGINT      NOT NULL DEFAULT 0
);
"""


class WalletDatabase:
    """
    Thin wrapper around the existing DB connection pool.
    All methods degrade silently to in-memory fallback when DB is down.
    """

    def __init__(self) -> None:
        self._conn                       = None
        self._fallback_wallets:   List[Dict]         = []
        self._fallback_balances:  Dict[str, float]   = {}
        self._fallback_profiles:  Dict[str, "WalletProfile"] = {}
        self._db_ok              = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def init(self) -> None:
        """Create tables if they don't exist. Called from __init__.py start_all()."""
        try:
            from services.db_pool import get_db
            self._conn  = get_db
            self._db_ok = True
            self._run(_CREATE_WALLETS)
            self._run(_CREATE_BALANCES)
            self._run(_CREATE_MOVEMENTS)
            self._run(_CREATE_PROFILES)
            logger.info("[WalletDB] Tables ready")
        except Exception as e:
            logger.warning(
                f"[WalletDB] Database unavailable ({e}) — using in-memory storage"
            )

    # ── Wallet registry ───────────────────────────────────────────────────────

    def upsert_wallet(self, wallet: Dict) -> None:
        sql = """
            INSERT INTO whale_wallets (address, label, chain, wallet_type, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE
                SET label = EXCLUDED.label,
                    wallet_type = EXCLUDED.wallet_type;
        """
        self._run(sql, (
            wallet["address"], wallet.get("label", ""),
            wallet.get("chain", "btc"), wallet.get("type", "unknown"),
            int(time.time() * 1000),
        ))
        # Keep fallback in sync
        if not any(w["address"] == wallet["address"]
                   for w in self._fallback_wallets):
            self._fallback_wallets.append(wallet)

    def load_all_wallets(self) -> List[Dict]:
        if not self._db_ok:
            return list(self._fallback_wallets)
        try:
            rows = self._query(
                "SELECT address, label, chain, wallet_type FROM whale_wallets;"
            )
            return [
                {"address": r[0], "label": r[1],
                 "chain": r[2], "type": r[3]}
                for r in (rows or [])
            ]
        except Exception as e:
            logger.debug(f"[WalletDB] load_all_wallets: {e}")
            return list(self._fallback_wallets)

    # ── Balance tracking ──────────────────────────────────────────────────────

    def update_balance(self, address: str, balance: float) -> None:
        self._fallback_balances[address] = balance
        sql = """
            INSERT INTO whale_balances (address, balance, updated_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (address) DO UPDATE
                SET balance = EXCLUDED.balance,
                    updated_at = EXCLUDED.updated_at;
        """
        self._run(sql, (address, balance, int(time.time() * 1000)))

    def get_balance(self, address: str) -> Optional[float]:
        if not self._db_ok:
            return self._fallback_balances.get(address)
        try:
            rows = self._query(
                "SELECT balance FROM whale_balances WHERE address = %s;",
                (address,)
            )
            return float(rows[0][0]) if rows else None
        except Exception as e:
            logger.debug(f"[WalletDB] get_balance: {e}")
            return self._fallback_balances.get(address)

    # ── Movement log ─────────────────────────────────────────────────────────

    def record_movement(self, address: str, delta: float, balance_after: float,
                        asset: str, event_type: str) -> None:
        sql = """
            INSERT INTO whale_movements
                (address, delta, balance_after, asset, event_type, ts)
            VALUES (%s, %s, %s, %s, %s, %s);
        """
        self._run(sql, (
            address, delta, balance_after,
            asset, event_type, int(time.time() * 1000),
        ))

    def get_movements(self, address: str, limit: int = 50) -> List[Dict]:
        if not self._db_ok:
            return []
        try:
            rows = self._query(
                """SELECT delta, balance_after, asset, event_type, ts
                   FROM whale_movements
                   WHERE address = %s
                   ORDER BY ts DESC LIMIT %s;""",
                (address, limit),
            )
            return [
                {"delta": r[0], "balance_after": r[1],
                 "asset": r[2], "event_type": r[3], "ts": r[4]}
                for r in (rows or [])
            ]
        except Exception as e:
            logger.debug(f"[WalletDB] get_movements: {e}")
            return []

    # ── Behaviour profiles ────────────────────────────────────────────────────

    def get_profile(self, address: str) -> Optional["WalletProfile"]:
        from whale_intelligence.wallet_behavior_classifier import WalletProfile
        if address in self._fallback_profiles:
            return self._fallback_profiles[address]
        if not self._db_ok:
            profile = WalletProfile(address=address)
            self._fallback_profiles[address] = profile
            return profile
        try:
            rows = self._query(
                "SELECT behavior, confidence, history_json, last_active "
                "FROM whale_profiles WHERE address = %s;",
                (address,),
            )
            if rows:
                r = rows[0]
                profile = WalletProfile(
                    address        = address,
                    behavior       = r[0],
                    confidence     = float(r[1]),
                    history        = json.loads(r[2]),
                    last_active_ts = int(r[3]),
                )
            else:
                profile = WalletProfile(address=address)
            self._fallback_profiles[address] = profile
            return profile
        except Exception as e:
            logger.debug(f"[WalletDB] get_profile: {e}")
            profile = WalletProfile(address=address)
            self._fallback_profiles[address] = profile
            return profile

    def update_profile(self, profile: "WalletProfile") -> None:
        self._fallback_profiles[profile.address] = profile
        sql = """
            INSERT INTO whale_profiles
                (address, behavior, confidence, history_json, last_active, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (address) DO UPDATE
                SET behavior     = EXCLUDED.behavior,
                    confidence   = EXCLUDED.confidence,
                    history_json = EXCLUDED.history_json,
                    last_active  = EXCLUDED.last_active,
                    updated_at   = EXCLUDED.updated_at;
        """
        # Only persist the last 200 history entries to keep JSON small
        trimmed = profile.history[-200:]
        self._run(sql, (
            profile.address,
            profile.behavior,
            profile.confidence,
            json.dumps(trimmed),
            profile.last_active_ts,
            int(time.time() * 1000),
        ))

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _run(self, sql: str, params: tuple = ()) -> None:
        if not self._db_ok:
            return
        try:
            from services.db_pool import get_db
            db = get_db()
            with db.get_session() as conn:
                conn.execute(text(sql), params)
        except Exception as e:
            logger.debug(f"[WalletDB] _run error: {e}")

    def _query(self, sql: str, params: tuple = ()) -> Optional[List]:
        if not self._db_ok:
            return None
        try:
            from services.db_pool import get_db
            db = get_db()
            with db.get_session() as conn:
                result = conn.execute(text(sql), params)
                return result.fetchall()
        except Exception as e:
            logger.debug(f"[WalletDB] _query error: {e}")
            return None
