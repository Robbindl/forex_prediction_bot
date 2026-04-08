from __future__ import annotations

import argparse
import json
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config.database import SessionLocal
from data.fetcher import DataFetcher
from models.trade_models import DailyStats, OpenPosition, Trade, TradingDiary
from risk.position_sizer import PositionSizer


STATE_FILE = Path("data/system_state.json")


def _dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dump_state(raw: Dict[str, Any]) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(raw, indent=2, default=str), encoding="utf-8")
    tmp.replace(STATE_FILE)


def _find_closed_trade(raw: Dict[str, Any], trade_id: str) -> Dict[str, Any]:
    for row in raw.get("closed_positions", []):
        if str(row.get("trade_id", "")) == trade_id:
            return row
    raise SystemExit(f"trade {trade_id} not found in {STATE_FILE}")


def _find_open_trade(raw: Dict[str, Any], trade_id: str) -> Dict[str, Any] | None:
    for row in raw.get("open_positions", []):
        if str(row.get("trade_id", "")) == trade_id:
            return row
    return None


def _load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        raise SystemExit(f"missing {STATE_FILE}")
    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def _managed_targets(trade: Dict[str, Any]) -> List[float]:
    levels: List[float] = []
    for raw_level in list(trade.get("take_profit_levels", []) or []):
        try:
            level = float(raw_level)
        except Exception:
            continue
        if level > 0:
            levels.append(round(level, 6))
    return levels


def _clean_open_trade_snapshot(trade: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = deepcopy(trade)
    cleaned.pop("exit_price", None)
    cleaned.pop("exit_reason", None)
    cleaned.pop("exit_time", None)
    cleaned.pop("duration_minutes", None)
    cleaned["pnl"] = 0.0
    metadata = dict(cleaned.get("metadata") or {})
    metadata.pop("execution_feedback", None)
    metadata.pop("post_trade_review", None)
    cleaned["metadata"] = metadata
    return cleaned


def _inconsistent_legacy_close(trade: Dict[str, Any]) -> bool:
    direction = str(trade.get("direction") or trade.get("signal") or "").upper()
    tp = float(trade.get("take_profit", 0) or 0)
    levels = _managed_targets(trade)
    if not tp or not levels or direction not in {"BUY", "SELL"}:
        return False
    first_level = levels[0]
    if direction == "BUY":
        return tp < first_level - 1e-6
    return tp > first_level + 1e-6


def _apply_trailing(pos: Dict[str, Any], entry: float, initial_risk: float, atr_value: float, direction: str, management: Dict[str, Any]) -> None:
    if initial_risk <= 0:
        return
    trail_activation_rr = max(0.5, float(management.get("trail_activation_rr", 1.0) or 1.0))
    trail_atr_multiple = max(0.4, float(management.get("trail_atr_multiple", 0.8) or 0.8))
    if direction == "BUY":
        favorable_extreme = float(pos.get("highest_price", entry) or entry)
        progress_rr = (favorable_extreme - entry) / max(initial_risk, 1e-9)
    else:
        favorable_extreme = float(pos.get("lowest_price", entry) or entry)
        progress_rr = (entry - favorable_extreme) / max(initial_risk, 1e-9)
    if progress_rr < trail_activation_rr:
        return
    trail_dist = max(initial_risk * 0.85, atr_value * trail_atr_multiple if atr_value > 0 else 0.0)
    if trail_dist <= 0:
        return
    current_sl = float(pos.get("stop_loss", 0) or 0)
    if direction == "BUY":
        trail_sl = favorable_extreme - trail_dist
        if trail_sl > current_sl:
            pos["stop_loss"] = round(trail_sl, 6)
    else:
        trail_sl = favorable_extreme + trail_dist
        if trail_sl < current_sl:
            pos["stop_loss"] = round(trail_sl, 6)


def _simulate_managed_trade(trade: Dict[str, Any], df) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
    trade = deepcopy(trade)
    direction = str(trade.get("direction") or trade.get("signal") or "").upper()
    entry = float(trade.get("entry_price", 0) or 0)
    stop_loss = float(trade.get("original_sl", trade.get("stop_loss", 0)) or 0)
    trade["stop_loss"] = round(stop_loss, 6)
    trade["take_profit_levels"] = _managed_targets(trade)
    if trade["take_profit_levels"]:
        trade["take_profit"] = float(trade["take_profit_levels"][-1])
        trade["original_take_profit"] = float(trade["take_profit_levels"][-1])
        trade["risk_reward"] = round(
            abs(float(trade["take_profit"]) - entry) / max(abs(entry - stop_loss), 1e-9),
            4,
        )
    trade["tp_hit"] = 0
    trade["pnl"] = 0.0
    trade.pop("exit_price", None)
    trade.pop("exit_time", None)
    trade.pop("exit_reason", None)
    trade.pop("duration_minutes", None)
    trade.pop("entry_time", None)
    trade["highest_price"] = max(float(trade.get("highest_price", entry) or entry), entry)
    trade["lowest_price"] = min(float(trade.get("lowest_price", entry) or entry), entry)

    metadata = dict(trade.get("metadata") or {})
    management = metadata.get("trade_management_plan") if isinstance(metadata.get("trade_management_plan"), dict) else {}
    initial_risk = abs(entry - stop_loss)
    atr_value = float(metadata.get("atr", 0.0) or 0.0)
    break_even_after_partial = bool(management.get("break_even_after_partial", False))
    levels = list(trade.get("take_profit_levels", []) or [])
    partials: List[Dict[str, Any]] = []

    for bar_time, bar in df.iterrows():
        bar_low = float(bar["low"])
        bar_high = float(bar["high"])
        trade["highest_price"] = max(float(trade.get("highest_price", entry) or entry), bar_high)
        trade["lowest_price"] = min(float(trade.get("lowest_price", entry) or entry), bar_low)
        current_stop = float(trade.get("stop_loss", stop_loss) or stop_loss)
        tp_idx = max(0, int(trade.get("tp_hit", 0) or 0))

        if direction == "BUY":
            if bar_low <= current_stop:
                trade["exit_price"] = round(current_stop, 6)
                trade["exit_reason"] = "Stop Loss (offline)"
                trade["exit_time"] = bar_time.isoformat()
                trade["duration_minutes"] = max(0, int((_dt(bar_time) - _dt(trade.get("open_time"))).total_seconds() / 60))
                trade["pnl"] = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, current_stop, float(trade["position_size"]), direction), 6)
                return "closed", trade, partials
            if levels and tp_idx < len(levels):
                tp_level = float(levels[tp_idx])
                if bar_high >= tp_level:
                    if tp_idx + 1 >= len(levels):
                        trade["exit_price"] = round(tp_level, 6)
                        trade["exit_reason"] = "Take Profit (offline)"
                        trade["exit_time"] = bar_time.isoformat()
                        trade["duration_minutes"] = max(0, int((_dt(bar_time) - _dt(trade.get("open_time"))).total_seconds() / 60))
                        trade["pnl"] = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, tp_level, float(trade["position_size"]), direction), 6)
                        return "closed", trade, partials
                    total_tiers = len(levels)
                    original_size = float(trade.get("position_size", 0) or 0)
                    close_fraction = 1.0 / max(1, total_tiers - tp_idx)
                    partial_size = round(original_size * close_fraction, 8)
                    remaining_size = round(max(0.0, original_size - partial_size), 8)
                    partial_pnl = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, tp_level, partial_size, direction), 6)
                    partials.append({
                        **deepcopy(trade),
                        "trade_id": f"{trade['trade_id']}-PT{tp_idx + 1}",
                        "parent_trade_id": trade["trade_id"],
                        "is_partial_close": True,
                        "position_size": partial_size,
                        "exit_price": round(tp_level, 6),
                        "exit_reason": f"Partial TP {tp_idx + 1}/{total_tiers} (offline)",
                        "exit_time": bar_time.isoformat(),
                        "duration_minutes": max(0, int((_dt(bar_time) - _dt(trade.get('open_time'))).total_seconds() / 60)),
                        "pnl": partial_pnl,
                        "metadata": {
                            **deepcopy(metadata),
                            "offline_gap_fill": {
                                "breach_time": bar_time.isoformat(),
                                "partial_tp_hit": int(tp_idx + 1),
                            },
                        },
                    })
                    trade["position_size"] = remaining_size
                    trade["tp_hit"] = tp_idx + 1
                    if break_even_after_partial and entry > float(trade.get("stop_loss", 0) or 0):
                        trade["stop_loss"] = round(entry, 6)
                    _apply_trailing(trade, entry, initial_risk, atr_value, direction, management)
                    continue
        else:
            if bar_high >= current_stop:
                trade["exit_price"] = round(current_stop, 6)
                trade["exit_reason"] = "Stop Loss (offline)"
                trade["exit_time"] = bar_time.isoformat()
                trade["duration_minutes"] = max(0, int((_dt(bar_time) - _dt(trade.get("open_time"))).total_seconds() / 60))
                trade["pnl"] = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, current_stop, float(trade["position_size"]), direction), 6)
                return "closed", trade, partials
            if levels and tp_idx < len(levels):
                tp_level = float(levels[tp_idx])
                if bar_low <= tp_level:
                    if tp_idx + 1 >= len(levels):
                        trade["exit_price"] = round(tp_level, 6)
                        trade["exit_reason"] = "Take Profit (offline)"
                        trade["exit_time"] = bar_time.isoformat()
                        trade["duration_minutes"] = max(0, int((_dt(bar_time) - _dt(trade.get("open_time"))).total_seconds() / 60))
                        trade["pnl"] = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, tp_level, float(trade["position_size"]), direction), 6)
                        return "closed", trade, partials
                    total_tiers = len(levels)
                    original_size = float(trade.get("position_size", 0) or 0)
                    close_fraction = 1.0 / max(1, total_tiers - tp_idx)
                    partial_size = round(original_size * close_fraction, 8)
                    remaining_size = round(max(0.0, original_size - partial_size), 8)
                    partial_pnl = round(PositionSizer.pnl(trade["asset"], trade["category"], entry, tp_level, partial_size, direction), 6)
                    partials.append({
                        **deepcopy(trade),
                        "trade_id": f"{trade['trade_id']}-PT{tp_idx + 1}",
                        "parent_trade_id": trade["trade_id"],
                        "is_partial_close": True,
                        "position_size": partial_size,
                        "exit_price": round(tp_level, 6),
                        "exit_reason": f"Partial TP {tp_idx + 1}/{total_tiers} (offline)",
                        "exit_time": bar_time.isoformat(),
                        "duration_minutes": max(0, int((_dt(bar_time) - _dt(trade.get('open_time'))).total_seconds() / 60)),
                        "pnl": partial_pnl,
                        "metadata": {
                            **deepcopy(metadata),
                            "offline_gap_fill": {
                                "breach_time": bar_time.isoformat(),
                                "partial_tp_hit": int(tp_idx + 1),
                            },
                        },
                    })
                    trade["position_size"] = remaining_size
                    trade["tp_hit"] = tp_idx + 1
                    if break_even_after_partial and entry < float(trade.get("stop_loss", 99e9) or 99e9):
                        trade["stop_loss"] = round(entry, 6)
                    _apply_trailing(trade, entry, initial_risk, atr_value, direction, management)
                    continue

        _apply_trailing(trade, entry, initial_risk, atr_value, direction, management)

    return "open", trade, partials


def _adjust_json_stats(raw: Dict[str, Any], trade: Dict[str, Any]) -> None:
    pnl = float(trade.get("pnl", 0.0) or 0.0)
    raw["balance"] = round(float(raw.get("balance", 0.0) or 0.0) - pnl, 6)
    raw["daily_pnl"] = round(float(raw.get("daily_pnl", 0.0) or 0.0) - pnl, 6)
    asset = str(trade.get("canonical_asset") or trade.get("asset") or "")
    strategy_id = str(trade.get("strategy_id") or "")
    session_key = str(trade.get("session") or "unknown")
    for bucket, key in (
        ("asset_stats", asset),
        ("strategy_stats", strategy_id),
        ("session_stats", session_key),
    ):
        row = raw.get(bucket, {}).get(key)
        if not row:
            continue
        row["pnl"] = round(float(row.get("pnl", 0.0) or 0.0) - pnl, 6)
        if pnl > 0 and int(row.get("wins", 0) or 0) > 0:
            row["wins"] = int(row.get("wins", 0) or 0) - 1
        elif pnl < 0 and int(row.get("losses", 0) or 0) > 0:
            row["losses"] = int(row.get("losses", 0) or 0) - 1
        if row.get("wins", 0) == 0 and row.get("losses", 0) == 0 and abs(float(row.get("pnl", 0.0) or 0.0)) < 1e-9:
            raw.get(bucket, {}).pop(key, None)


def _repair_state_json(trade_id: str, corrected_open: Dict[str, Any], original_trade: Dict[str, Any]) -> None:
    raw = _load_state()
    raw["closed_positions"] = [
        row for row in raw.get("closed_positions", [])
        if str(row.get("trade_id", "")) != trade_id and not str(row.get("trade_id", "")).startswith(f"{trade_id}-PT")
    ]
    raw["open_positions"] = [
        row for row in raw.get("open_positions", [])
        if str(row.get("trade_id", "")) != trade_id
    ]
    raw["open_positions"].append(corrected_open)
    raw.get("cooldowns", {}).pop(str(corrected_open.get("canonical_asset") or corrected_open.get("asset") or ""), None)
    _adjust_json_stats(raw, original_trade)
    _dump_state(raw)


def _replace_open_state_snapshot(trade_id: str, corrected_open: Dict[str, Any]) -> None:
    raw = _load_state()
    replaced = False
    for idx, row in enumerate(list(raw.get("open_positions", []))):
        if str(row.get("trade_id", "")) == trade_id:
            raw["open_positions"][idx] = corrected_open
            replaced = True
            break
    if not replaced:
        raw.setdefault("open_positions", []).append(corrected_open)
    _dump_state(raw)


def _repair_db(trade_id: str, corrected_open: Dict[str, Any], original_trade: Dict[str, Any]) -> None:
    balance = float(_load_state().get("balance", 0.0) or 0.0)
    pnl = float(original_trade.get("pnl", 0.0) or 0.0)
    entry_time = _dt(original_trade.get("entry_time") or original_trade.get("open_time"))
    day = entry_time.date().isoformat() if entry_time else datetime.now(timezone.utc).date().isoformat()
    session = SessionLocal()
    try:
        session.query(TradingDiary).filter(TradingDiary.trade_id.like(f"{trade_id}%")).delete(synchronize_session=False)
        session.query(Trade).filter(Trade.trade_id.like(f"{trade_id}%")).delete(synchronize_session=False)
        session.query(OpenPosition).filter(OpenPosition.trade_id == trade_id).delete(synchronize_session=False)
        row = OpenPosition(
            trade_id=str(corrected_open["trade_id"]),
            asset=str(corrected_open.get("asset", "")),
            canonical_asset=str(corrected_open.get("canonical_asset", "")),
            category=str(corrected_open.get("category", "forex")),
            direction=str(corrected_open.get("direction", corrected_open.get("signal", "BUY"))),
            entry_price=float(corrected_open.get("entry_price", 0) or 0),
            stop_loss=float(corrected_open.get("stop_loss", 0) or 0),
            take_profit=float(corrected_open.get("take_profit", 0) or 0),
            position_size=float(corrected_open.get("position_size", 0) or 0),
            confidence=float(corrected_open.get("confidence", 0) or 0),
            strategy_id=str(corrected_open.get("strategy_id", "")),
            position_data=corrected_open,
        )
        session.add(row)
        daily = session.query(DailyStats).filter(DailyStats.date == day).first()
        if daily:
            daily.trade_count = max(0, int(daily.trade_count or 0) - 1)
            daily.pnl = float(daily.pnl or 0.0) - pnl
            daily.balance_end = balance
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _load_db_trade(trade_id: str) -> Dict[str, Any] | None:
    session = SessionLocal()
    try:
        row = session.query(Trade).filter(Trade.trade_id == trade_id).first()
        if row is None:
            return None
        return row.to_dict()
    finally:
        session.close()


def repair_trade(trade_id: str) -> Dict[str, Any]:
    raw = _load_state()
    state_open = _find_open_trade(raw, trade_id)
    if state_open:
        original_trade = _load_db_trade(trade_id)
        corrected_trade = _clean_open_trade_snapshot(state_open)
        _replace_open_state_snapshot(trade_id, corrected_trade)
        if not original_trade:
            return {
                "trade_id": trade_id,
                "status": "open",
                "restored_position_size": corrected_trade.get("position_size"),
                "restored_stop_loss": corrected_trade.get("stop_loss"),
                "restored_take_profit": corrected_trade.get("take_profit"),
                "tp_hit": corrected_trade.get("tp_hit"),
                "highest_price": corrected_trade.get("highest_price"),
                "lowest_price": corrected_trade.get("lowest_price"),
                "removed_false_pnl": None,
                "replayed_bars": None,
                "partials_detected": None,
            }
        _repair_db(trade_id, corrected_trade, original_trade)
        return {
            "trade_id": trade_id,
            "status": "open",
            "restored_position_size": corrected_trade.get("position_size"),
            "restored_stop_loss": corrected_trade.get("stop_loss"),
            "restored_take_profit": corrected_trade.get("take_profit"),
            "tp_hit": corrected_trade.get("tp_hit"),
            "highest_price": corrected_trade.get("highest_price"),
            "lowest_price": corrected_trade.get("lowest_price"),
            "removed_false_pnl": original_trade.get("pnl"),
            "replayed_bars": None,
            "partials_detected": None,
        }

    trade = deepcopy(_find_closed_trade(raw, trade_id))
    if not _inconsistent_legacy_close(trade):
        raise SystemExit(f"trade {trade_id} does not look like a legacy managed-target mismatch")

    fetcher = DataFetcher()
    open_time = _dt(trade.get("open_time") or trade.get("entry_time"))
    now_utc = datetime.now(timezone.utc)
    periods = max(48, int((now_utc - open_time).total_seconds() // 300) + 24)
    df = fetcher.get_ohlcv(str(trade.get("asset", "")), str(trade.get("category", "forex")), interval="5m", periods=periods)
    if df is None or df.empty:
        raise SystemExit("no historical 5m data available for repair")
    df = df[df.index > open_time].copy()
    if df.empty:
        raise SystemExit("no post-entry bars available for repair")

    status, corrected_trade, partials = _simulate_managed_trade(trade, df)
    corrected_trade["metadata"] = {
        **dict(corrected_trade.get("metadata") or {}),
        "legacy_managed_trade_repair": {
            "repaired_at_utc": datetime.now(timezone.utc).isoformat(),
            "original_trade_id": trade_id,
            "repair_status": status,
            "replayed_bar_count": int(len(df)),
        },
    }

    if status != "open":
        raise SystemExit(f"repair simulation for {trade_id} produced status={status}; script currently only restores still-open trades")

    corrected_trade = _clean_open_trade_snapshot(corrected_trade)
    _repair_state_json(trade_id, corrected_trade, trade)
    _repair_db(trade_id, corrected_trade, trade)

    return {
        "trade_id": trade_id,
        "status": status,
        "restored_position_size": corrected_trade.get("position_size"),
        "restored_stop_loss": corrected_trade.get("stop_loss"),
        "restored_take_profit": corrected_trade.get("take_profit"),
        "tp_hit": corrected_trade.get("tp_hit"),
        "highest_price": corrected_trade.get("highest_price"),
        "lowest_price": corrected_trade.get("lowest_price"),
        "removed_false_pnl": trade.get("pnl"),
        "replayed_bars": int(len(df)),
        "partials_detected": len(partials),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair legacy managed trade history rows affected by flat offline TP closes.")
    parser.add_argument("trade_id", help="trade_id to repair")
    args = parser.parse_args()
    result = repair_trade(args.trade_id)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
