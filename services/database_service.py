"""
services/database_service.py — All database operations.
Database is REQUIRED. Methods raise on failure — no silent skipping.
"""
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


def _np(value: Any) -> Any:
    """Convert numpy scalars to Python native types for SQLAlchemy."""
    if value is None:
        return None
    if isinstance(value, np.floating):  return float(value)
    if isinstance(value, np.integer):   return int(value)
    if isinstance(value, np.bool_):     return bool(value)
    if isinstance(value, np.ndarray):   return value.tolist()
    return value


class DatabaseService:
    """
    Single class for all DB operations.
    Each public method opens its own session and commits/rolls back.
    Thread-safe — no shared session state.
    """

    # ── Session context manager ───────────────────────────────────────────────

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
                existing.position_data = position
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
                    datetime.fromisoformat(trade_data["exit_time"])
                    if trade_data.get("exit_time") else datetime.utcnow()
                )
                existing.exit_reason = str(_np(trade_data.get("exit_reason", "")))
                existing.pnl         = _np(trade_data.get("pnl"))
                existing.pnl_percent = _np(trade_data.get("pnl_percent"))
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
                    datetime.fromisoformat(trade_data["exit_time"])
                    if trade_data.get("exit_time") else None
                ),
                exit_reason     = str(_np(trade_data.get("exit_reason", ""))) if trade_data.get("exit_reason") else None,
                strategy_id     = str(_np(trade_data.get("strategy_id", "UNKNOWN"))),
                confidence      = _np(trade_data.get("confidence", 0)),
                trade_metadata  = trade_data.get("metadata", {}),
            )
            s.add(row)
        logger.debug(f"[DB] Trade saved: {tid}")
        return tid

    def get_recent_trades(self, limit: int = 50) -> List[Dict]:
        with self.get_session() as s:
            rows = (
                s.query(Trade)
                .order_by(desc(Trade.entry_time))
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

    def upsert_daily_stats(self, date_str: str, pnl_delta: float, balance: float) -> None:
        """Update today's daily stats row. Creates if missing."""
        with self.get_session() as s:
            row = s.query(DailyStats).filter_by(date=date_str).first()
            if row:
                row.trade_count += 1
                row.pnl          = float(row.pnl or 0) + pnl_delta
                row.balance_end  = balance
            else:
                s.add(DailyStats(
                    date        = date_str,
                    trade_count = 1,
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

    def save_whale_alert(self, alert: Dict) -> None:
        with self.get_session() as s:
            row = WhaleAlert(
                title      = str(alert.get("title", "")),
                symbol     = str(alert.get("symbol", alert.get("asset", ""))),
                value_usd  = _np(alert.get("value_usd", alert.get("size_usd", 0))),
                source     = str(alert.get("source", "")),
                direction  = str(alert.get("direction", "")),
                alert_time = datetime.utcnow(),
            )
            s.add(row)

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

    # ── Health check ──────────────────────────────────────────────────────────

    def ping(self) -> bool:
        try:
            with self.get_session() as s:
                s.execute(text("SELECT 1"))
            return True
        except Exception:
            return False