from __future__ import annotations
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import desc, func, text
from sqlalchemy.orm import Session

from config.database import SessionLocal
from models.trade_models import (
    Trade, OpenPosition, DailyStats,
    TradingDiary, WhaleAlert, BotPersonality,
)
from utils.logger import get_logger

logger = get_logger()

_CREATE_STRATEGY_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS strategy_performance (
    asset        TEXT NOT NULL,
    category     TEXT NOT NULL,
    strategy_id  TEXT NOT NULL,
    win_rate     REAL NOT NULL DEFAULT 0,
    sharpe_ratio REAL NOT NULL DEFAULT 0,
    total_trades INT  NOT NULL DEFAULT 0,
    recorded_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset, strategy_id)
);
"""

_CREATE_STRATEGY_OPTIMISATION = """
CREATE TABLE IF NOT EXISTS strategy_optimisation (
    asset         TEXT NOT NULL,
    category      TEXT NOT NULL,
    best_params   TEXT NOT NULL DEFAULT '{}',
    sharpe        REAL NOT NULL DEFAULT 0,
    win_rate      REAL NOT NULL DEFAULT 0,
    optimised_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset, category)
);
"""

_CREATE_PREDICTION_OUTCOMES = """
CREATE TABLE IF NOT EXISTS prediction_outcomes (
    id                SERIAL PRIMARY KEY,
    asset             TEXT NOT NULL,
    category          TEXT,
    direction         TEXT NOT NULL,
    entry_price       FLOAT,
    target_price      FLOAT,
    confidence        FLOAT,
    signal_time       TIMESTAMP NOT NULL,
    horizon_minutes   INT NOT NULL,
    eval_time         TIMESTAMP,
    actual_price      FLOAT,
    direction_correct BOOLEAN,
    target_hit        BOOLEAN,
    pct_move          FLOAT,
    evaluated         BOOLEAN DEFAULT FALSE,
    strategy          TEXT,
    session           TEXT,
    regime            TEXT,
    signal_features   TEXT,
    signal_metadata   TEXT
);
"""

_PREDICTION_OUTCOME_MIGRATIONS = (
    "ALTER TABLE prediction_outcomes ADD COLUMN IF NOT EXISTS signal_features TEXT",
    "ALTER TABLE prediction_outcomes ADD COLUMN IF NOT EXISTS signal_metadata TEXT",
    "CREATE INDEX IF NOT EXISTS idx_prediction_outcomes_eval_signal_time ON prediction_outcomes (evaluated, signal_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_prediction_outcomes_category_eval_signal_time ON prediction_outcomes (category, evaluated, signal_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_prediction_outcomes_asset_horizon_signal_time ON prediction_outcomes (asset, horizon_minutes, signal_time)",
)


def _np(value: Any) -> Any:
    """Convert numpy scalars to Python native types for SQLAlchemy."""
    if value is None:
        return None
    if isinstance(value, np.floating):  return float(value)
    if isinstance(value, np.integer):   return int(value)
    if isinstance(value, np.bool_):     return bool(value)
    if isinstance(value, np.ndarray):   return value.tolist()
    return value


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


class DatabaseService:
    """
    Single class for all DB operations.
    Each public method opens its own session and commits/rolls back.
    Thread-safe — no shared session state.
    """

    # ── Session context manager ───────────────────────────────────────────────

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    @contextmanager
    def get_session(self):
        session: Session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── Open positions (restart recovery) ────────────────────────────────────

    def save_open_position(self, position: Dict) -> None:
        """Write open position to DB. Call on every trade OPEN."""
        with self.get_session() as s:
            existing = s.query(OpenPosition).filter_by(
                trade_id=position["trade_id"]
            ).first()
            if existing:
                # FIX M-06: Update all columns, including stop_loss (for trailing stop updates)
                existing.position_data = position
                existing.asset           = str(position.get("asset", ""))
                existing.canonical_asset = str(position.get("canonical_asset", ""))
                existing.category        = str(position.get("category", "forex"))
                existing.direction       = str(position.get("direction") or position.get("signal", "BUY"))
                existing.entry_price     = _np(position.get("entry_price", 0))
                existing.stop_loss       = _np(position.get("stop_loss", 0))  # Critical for trailing stops
                existing.take_profit     = _np(position.get("take_profit", 0))
                existing.position_size   = _np(position.get("position_size", 0))
                existing.confidence      = _np(position.get("confidence", 0))
                existing.strategy_id     = str(position.get("strategy_id", ""))
                return
            row = OpenPosition(
                trade_id        = str(position["trade_id"]),
                asset           = str(position.get("asset", "")),
                canonical_asset = str(position.get("canonical_asset", "")),
                category        = str(position.get("category", "forex")),
                direction       = str(position.get("direction") or position.get("signal", "BUY")),
                entry_price     = _np(position.get("entry_price", 0)),
                stop_loss       = _np(position.get("stop_loss", 0)),
                take_profit     = _np(position.get("take_profit", 0)),
                position_size   = _np(position.get("position_size", 0)),
                confidence      = _np(position.get("confidence", 0)),
                strategy_id     = str(position.get("strategy_id", "")),
                position_data   = position,
            )
            s.add(row)
        logger.debug(f"[DB] OpenPosition saved: {position['trade_id']}")

    def delete_open_position(self, trade_id: str) -> None:
        """Remove open position row when trade closes."""
        with self.get_session() as s:
            s.query(OpenPosition).filter_by(trade_id=trade_id).delete()

    def load_open_positions(self) -> List[Dict]:
        """Load all open positions on startup for restart recovery."""
        with self.get_session() as s:
            rows = s.query(OpenPosition).all()
            return [r.to_dict() for r in rows]

    # ── Trades ────────────────────────────────────────────────────────────────

    def save_trade(self, trade_data: Dict) -> str:
        """Persist a closed trade. Returns trade_id."""
        tid = str(_np(trade_data.get("trade_id", str(uuid.uuid4())[:12])))
        with self.get_session() as s:
            existing = s.query(Trade).filter_by(trade_id=tid).first()
            if existing:
                # Update exit fields on existing record
                existing.exit_price  = _np(trade_data.get("exit_price"))
                existing.exit_time   = (
                    datetime.fromisoformat(trade_data["exit_time"].replace('Z','').replace('+00:00',''))
                    if trade_data.get("exit_time") else datetime.utcnow()
                )
                existing.exit_reason = str(_np(trade_data.get("exit_reason", "")))
                existing.pnl         = _np(trade_data.get("pnl"))
                existing.pnl_percent = _np(trade_data.get("pnl_percent"))
                existing.duration_minutes = int(_np(trade_data.get("duration_minutes", 0)))
                return tid

            row = Trade(
                trade_id        = tid,
                asset           = str(_np(trade_data.get("asset", "UNKNOWN"))),
                canonical_asset = str(_np(trade_data.get("canonical_asset", ""))),
                category        = str(_np(trade_data.get("category", "unknown"))),
                direction       = str(_np(trade_data.get("direction") or trade_data.get("signal", "BUY"))),
                entry_price     = _np(trade_data.get("entry_price", 0)),
                exit_price      = _np(trade_data.get("exit_price")),
                position_size   = _np(trade_data.get("position_size", 0)),
                stop_loss       = _np(trade_data.get("stop_loss", 0)),
                take_profit     = _np(trade_data.get("take_profit")),
                pnl             = _np(trade_data.get("pnl")),
                pnl_percent     = _np(trade_data.get("pnl_percent")),
                exit_time       = (
                    datetime.fromisoformat(trade_data["exit_time"].replace('Z','').replace('+00:00',''))
                    if trade_data.get("exit_time") else None
                ),
                entry_time      = (
                    datetime.fromisoformat(trade_data["open_time"].replace('Z','').replace('+00:00',''))
                    if trade_data.get("open_time") else datetime.utcnow()
                ),
                exit_reason     = str(_np(trade_data.get("exit_reason", ""))) if trade_data.get("exit_reason") else None,
                strategy_id     = str(_np(trade_data.get("strategy_id", "UNKNOWN"))),
                confidence      = _np(trade_data.get("confidence", 0)),
                duration_minutes = int(_np(trade_data.get("duration_minutes", 0))),
                trade_metadata  = trade_data.get("metadata", {}),
            )
            s.add(row)
        logger.debug(f"[DB] Trade saved: {tid}")
        return tid

    def get_recent_trades(
        self,
        limit: int = 50,
        category: str = "",
        pnl_filter: str = "all",
    ) -> List[Dict]:
        with self.get_session() as s:
            query = s.query(Trade).filter(Trade.exit_time.isnot(None))
            if category:
                query = query.filter(Trade.category == category)
            if pnl_filter == "won":
                query = query.filter(Trade.pnl > 0)
            elif pnl_filter == "lost":
                query = query.filter(Trade.pnl < 0)

            rows = (
                query.order_by(desc(func.coalesce(Trade.exit_time, Trade.entry_time)))
                .limit(limit)
                .all()
            )
            return [r.to_dict() for r in rows]

    def get_trades_since(self, since: datetime) -> List[Dict]:
        with self.get_session() as s:
            rows = (
                s.query(Trade)
                .filter(Trade.entry_time >= since)
                .order_by(Trade.entry_time)
                .all()
            )
            return [r.to_dict() for r in rows]
    def clear_trade_history(self, clear_daily_stats: bool = True) -> None:
        """Delete all closed trade history and optionally daily stats."""
        with self.get_session() as s:
            s.query(TradingDiary).delete()
            s.query(Trade).delete()
            if clear_daily_stats:
                s.query(DailyStats).delete()

    def get_closed_trade_rollups(self) -> Dict[str, Dict[str, Dict[str, float]]]:
        """Aggregate closed-trade win/loss/PnL stats for state rebuilds."""
        with self.get_session() as s:
            rows = s.execute(text("""
                SELECT strategy_id, canonical_asset, pnl
                FROM   trades
                WHERE  exit_time IS NOT NULL
                  AND  pnl IS NOT NULL
            """)).fetchall()

        strategy_rollups: Dict[str, Dict[str, float]] = {}
        asset_rollups: Dict[str, Dict[str, float]] = {}

        for strategy_id, canonical_asset, pnl_raw in rows:
            pnl = float(pnl_raw)
            win = pnl > 0

            strategy_key = str(strategy_id or "").strip()
            if strategy_key:
                strategy_stats = strategy_rollups.setdefault(
                    strategy_key,
                    {"wins": 0, "losses": 0, "pnl": 0.0},
                )
                strategy_stats["pnl"] += pnl
                if win:
                    strategy_stats["wins"] += 1
                else:
                    strategy_stats["losses"] += 1

            asset_key = str(canonical_asset or "").strip()
            if asset_key:
                asset_stats = asset_rollups.setdefault(
                    asset_key,
                    {"wins": 0, "losses": 0, "pnl": 0.0},
                )
                asset_stats["pnl"] += pnl
                if win:
                    asset_stats["wins"] += 1
                else:
                    asset_stats["losses"] += 1

        return {
            "rows": rows,
            "strategy": strategy_rollups,
            "asset": asset_rollups,
        }
    # ── Performance ───────────────────────────────────────────────────────────

    def get_performance_summary(self, days: int = 30) -> Dict:
        cutoff = datetime.utcnow() - timedelta(days=days)
        with self.get_session() as s:
            trades = (
                s.query(Trade)
                .filter(Trade.entry_time >= cutoff, Trade.exit_time.isnot(None))
                .all()
            )
        if not trades:
            return {"period_days": days, "total_trades": 0}

        total   = len(trades)
        wins    = [t for t in trades if t.pnl and float(t.pnl) > 0]
        total_pnl = sum(float(t.pnl) for t in trades if t.pnl)

        return {
            "period_days":    days,
            "total_trades":   total,
            "winning_trades": len(wins),
            "win_rate":       round(len(wins) / total, 4) if total else 0,
            "total_pnl":      round(total_pnl, 4),
            "avg_pnl":        round(total_pnl / total, 4) if total else 0,
        }

    # ── Daily stats ───────────────────────────────────────────────────────────

    def upsert_daily_stats(
        self,
        date_str: str,
        pnl_delta: float,
        balance: float,
        trade_count_delta: int = 1,
    ) -> None:
        """Update today's daily stats row. Creates if missing."""
        with self.get_session() as s:
            row = s.query(DailyStats).filter_by(date=date_str).first()
            if row:
                row.trade_count += trade_count_delta
                row.pnl          = float(row.pnl or 0) + pnl_delta
                row.balance_end  = balance
            else:
                s.add(DailyStats(
                    date        = date_str,
                    trade_count = trade_count_delta,
                    pnl         = pnl_delta,
                    balance_end = balance,
                ))

    def get_daily_stats(self, days: int = 7) -> List[Dict]:
        with self.get_session() as s:
            rows = (
                s.query(DailyStats)
                .order_by(desc(DailyStats.date))
                .limit(days)
                .all()
            )
            return [
                {
                    "date":        r.date,
                    "trade_count": r.trade_count,
                    "pnl":         float(r.pnl or 0),
                    "balance_end": float(r.balance_end or 0),
                }
                for r in rows
            ]

    # ── Whale alerts ──────────────────────────────────────────────────────────

    def save_whale_alert(self, alert: Dict) -> bool:
        alert_time = _coerce_datetime(alert.get("alert_time") or alert.get("date")) or datetime.utcnow()
        with self.get_session() as s:
            exists = (
                s.query(WhaleAlert)
                .filter(
                    WhaleAlert.title == str(alert.get("title", "")),
                    WhaleAlert.alert_time == alert_time,
                )
                .first()
            )
            if exists:
                return False
            row = WhaleAlert(
                title      = str(alert.get("title", "")),
                symbol     = str(alert.get("symbol", alert.get("asset", ""))),
                value_usd  = _np(alert.get("value_usd", alert.get("size_usd", 0))),
                source     = str(alert.get("source", "")),
                direction  = str(alert.get("direction", "")),
                alert_time = alert_time,
            )
            s.add(row)
            return True

    def get_recent_whale_alerts(self, hours: int = 24, symbol: str = "") -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        with self.get_session() as s:
            q = s.query(WhaleAlert).filter(WhaleAlert.alert_time >= cutoff)
            if symbol:
                q = q.filter(WhaleAlert.symbol.ilike(f"%{symbol}%"))
            return [r.to_dict() for r in q.order_by(desc(WhaleAlert.alert_time)).all()]

    # ── Balance ───────────────────────────────────────────────────────────────

    def get_current_balance(self) -> Optional[float]:
        """Read latest balance from most recent daily_stats row."""
        with self.get_session() as s:
            row = s.query(DailyStats).order_by(desc(DailyStats.date)).first()
            return float(row.balance_end) if row and row.balance_end else None

    # ── Strategy reporting ───────────────────────────────────────────────────

    def ensure_strategy_reporting_tables(self) -> None:
        with self.get_session() as s:
            s.execute(text(_CREATE_STRATEGY_PERFORMANCE))
            s.execute(text(_CREATE_STRATEGY_OPTIMISATION))

    # ── Prediction outcomes ──────────────────────────────────────────────────

    def ensure_prediction_outcomes_table(self) -> None:
        with self.get_session() as s:
            s.execute(text(_CREATE_PREDICTION_OUTCOMES))
            for stmt in _PREDICTION_OUTCOME_MIGRATIONS:
                s.execute(text(stmt))

    def save_prediction_outcomes(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        with self.get_session() as s:
            for record in records:
                s.execute(text("""
                    INSERT INTO prediction_outcomes
                        (asset, category, direction, entry_price, target_price,
                         confidence, signal_time, horizon_minutes, eval_time,
                         strategy, session, regime, signal_features,
                         signal_metadata, evaluated)
                    VALUES
                        (:asset, :category, :direction, :entry_price, :target_price,
                         :confidence, :signal_time, :horizon_minutes, :eval_time,
                         :strategy, :session, :regime, :signal_features,
                         :signal_metadata, false)
                """), {
                    "asset": record["asset"],
                    "category": record.get("category", ""),
                    "direction": record["direction"],
                    "entry_price": record["entry_price"],
                    "target_price": record.get("target_price"),
                    "confidence": record["confidence"],
                    "signal_time": record["signal_time"],
                    "horizon_minutes": record["horizon_minutes"],
                    "eval_time": record["eval_time"],
                    "strategy": record.get("strategy", ""),
                    "session": record.get("session", ""),
                    "regime": record.get("regime", ""),
                    "signal_features": record.get("signal_features"),
                    "signal_metadata": record.get("signal_metadata"),
                })

    def mark_prediction_outcome_evaluated(self, record: Dict[str, Any]) -> None:
        with self.get_session() as s:
            s.execute(text("""
                UPDATE prediction_outcomes SET
                    actual_price      = :actual,
                    direction_correct = :correct,
                    target_hit        = :hit,
                    pct_move          = :move,
                    evaluated         = true
                WHERE asset = :asset
                  AND signal_time = :signal_time
                  AND horizon_minutes = :horizon
            """), {
                "actual": record["actual_price"],
                "correct": record["direction_correct"],
                "hit": record["target_hit"],
                "move": record["pct_move"],
                "asset": record["asset"],
                "signal_time": record["signal_time"],
                "horizon": record["horizon_minutes"],
            })

    def get_prediction_accuracy_rollups(
        self,
        since: datetime,
        asset_limit: int = 50,
        recent_limit: int = 20,
    ) -> Dict[str, List[Any]]:
        with self.get_session() as s:
            horizon_rows = s.execute(text("""
                SELECT
                    horizon_minutes,
                    COUNT(*) AS total,
                    SUM(CASE WHEN direction_correct THEN 1 ELSE 0 END) AS correct,
                    SUM(CASE WHEN target_hit THEN 1 ELSE 0 END) AS targets_hit,
                    AVG(pct_move) AS avg_move,
                    AVG(confidence) AS avg_confidence
                FROM prediction_outcomes
                WHERE evaluated = true AND signal_time >= :since
                GROUP BY horizon_minutes
            """), {"since": since}).fetchall()

            asset_rows = s.execute(text("""
                SELECT
                    asset,
                    horizon_minutes,
                    COUNT(*) AS total,
                    SUM(CASE WHEN direction_correct THEN 1 ELSE 0 END) AS correct
                FROM prediction_outcomes
                WHERE evaluated = true AND signal_time >= :since
                GROUP BY asset, horizon_minutes
                ORDER BY total DESC
                LIMIT :asset_limit
            """), {"since": since, "asset_limit": asset_limit}).fetchall()

            recent_rows = s.execute(text("""
                SELECT asset, direction, entry_price, actual_price,
                       direction_correct, pct_move, confidence, horizon_minutes, signal_time
                FROM prediction_outcomes
                WHERE evaluated = true
                ORDER BY signal_time DESC
                LIMIT :recent_limit
            """), {"recent_limit": recent_limit}).fetchall()

        return {
            "by_horizon": horizon_rows,
            "by_asset": asset_rows,
            "recent": recent_rows,
        }

    def get_pending_prediction_outcomes(self, lookback_days: int) -> List[Dict[str, Any]]:
        since = datetime.utcnow() - timedelta(days=lookback_days)
        with self.get_session() as s:
            rows = s.execute(text("""
                SELECT asset, category, direction, entry_price, target_price,
                       confidence, signal_time, horizon_minutes, eval_time,
                       strategy, session, regime, signal_features,
                       signal_metadata
                FROM prediction_outcomes
                WHERE evaluated = false
                  AND signal_time >= :since
            """), {"since": since}).fetchall()

        cols = [
            "asset", "category", "direction", "entry_price", "target_price",
            "confidence", "signal_time", "horizon_minutes", "eval_time",
            "strategy", "session", "regime", "signal_features",
            "signal_metadata",
        ]
        return [dict(zip(cols, row)) for row in rows]

    def get_live_prediction_training_rows(
        self,
        category: str,
        since: datetime,
        limit: int = 2000,
    ) -> List[Any]:
        with self.get_session() as s:
            rows = s.execute(text("""
                SELECT entry_price, actual_price, signal_features, signal_metadata
                FROM prediction_outcomes
                WHERE evaluated = true
                  AND category = :category
                  AND signal_features IS NOT NULL
                  AND signal_time >= :since
                ORDER BY signal_time DESC
                LIMIT :limit
            """), {
                "category": category,
                "since": since,
                "limit": limit,
            }).fetchall()
        return rows

    def save_strategy_performance_snapshot(
        self,
        asset: str,
        category: str,
        strategy_id: str,
        win_rate: float,
        sharpe_ratio: float,
        total_trades: int,
    ) -> None:
        with self.get_session() as s:
            s.execute(text("""
                INSERT INTO strategy_performance
                    (asset, category, strategy_id, win_rate, sharpe_ratio,
                     total_trades, recorded_at)
                VALUES (:asset, :category, :strategy_id, :win_rate, :sharpe_ratio,
                        :total_trades, NOW())
                ON CONFLICT (asset, strategy_id)
                DO UPDATE SET
                    category     = EXCLUDED.category,
                    win_rate     = EXCLUDED.win_rate,
                    sharpe_ratio = EXCLUDED.sharpe_ratio,
                    total_trades = EXCLUDED.total_trades,
                    recorded_at  = NOW();
            """), {
                "asset": asset,
                "category": category,
                "strategy_id": strategy_id,
                "win_rate": win_rate,
                "sharpe_ratio": sharpe_ratio,
                "total_trades": total_trades,
            })

    def save_strategy_optimisation_result(self, asset: str, category: str, result: Dict[str, Any]) -> None:
        with self.get_session() as s:
            s.execute(text("""
                INSERT INTO strategy_optimisation
                    (asset, category, best_params, sharpe, win_rate, optimised_at)
                VALUES (:asset, :category, :best_params, :sharpe, :win_rate, NOW())
                ON CONFLICT (asset, category)
                DO UPDATE SET
                    best_params  = EXCLUDED.best_params,
                    sharpe       = EXCLUDED.sharpe,
                    win_rate     = EXCLUDED.win_rate,
                    optimised_at = NOW();
            """), {
                "asset": asset,
                "category": category,
                "best_params": json.dumps(result),
                "sharpe": float(result.get("sharpe", 0.0) or 0.0),
                "win_rate": float(result.get("win_rate", 0.0) or 0.0),
            })

    # ── Health check ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        try:
            with self.get_session() as s:
                s.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
