from __future__ import annotations
import uuid
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from risk.manager import RiskManager
from utils.logger import get_logger

logger = get_logger()


class PaperTrader:
    """
    Executes and monitors paper trades.
    Fires on_trade_closed(trade_dict) callback when a position closes.
    """

    def __init__(
        self,
        account_balance: float = 30.0,
        risk_manager: Optional[RiskManager] = None,
    ):
        self.account_balance      = account_balance
        self._risk_manager        = risk_manager or RiskManager(account_balance)
        self.open_positions:  Dict[str, Dict] = {}
        self._lock                = threading.RLock()
        self.on_trade_closed: Optional[Callable[[Dict], None]] = None

    # ── Restore persisted positions on restart ────────────────────────────────

    def restore_position(self, pos: Dict) -> None:
        tid = pos.get("trade_id")
        if tid:
            with self._lock:
                self.open_positions[tid] = pos

    # ── Execute a signal ──────────────────────────────────────────────────────

    def execute_signal(self, signal: Dict) -> Optional[Dict]:
        """
        Open a paper trade from a signal dict.
        Returns trade dict or None if rejected.
        """
        asset      = signal.get("asset", "")
        direction  = signal.get("direction") or signal.get("signal", "BUY")
        confidence = float(signal.get("confidence", 0.5))
        entry      = float(signal.get("entry_price", 0))
        stop_loss  = float(signal.get("stop_loss", 0))
        take_profit= float(signal.get("take_profit", 0))
        tp_levels  = signal.get("take_profit_levels", [])
        category   = signal.get("category", "forex")
        strategy   = signal.get("strategy_id", "UNKNOWN")

        if not entry or not stop_loss:
            logger.warning(f"[PaperTrader] Missing price data for {asset}")
            return None

        # Position size with asset-aware pip values
        pos_size = float(signal.get("position_size", 0))
        if not pos_size:
            pos_size = self._risk_manager.calculate_position_size(
                entry_price=entry,
                stop_loss=stop_loss,
                category=category,
                confidence=confidence,
                asset=asset,  # Pass asset for pip value calculation
            )

        if not pos_size or pos_size <= 0:
            return None

        trade_id = str(uuid.uuid4())[:12]
        trade    = {
            "trade_id":           trade_id,
            "asset":              asset,
            "canonical_asset":    signal.get("canonical_asset", asset),
            "category":           category,
            "signal":             direction,
            "direction":          direction,
            "confidence":         round(confidence, 4),
            "entry_price":        entry,
            "stop_loss":          stop_loss,
            "original_sl":        stop_loss,   # preserved for trailing stop detection
            "take_profit":        take_profit,
            "take_profit_levels": tp_levels,
            "position_size":      pos_size,
            "strategy_id":        strategy,
            "open_time":          datetime.utcnow().isoformat(),
            "pnl":                0.0,
            "highest_price":      entry,
            "lowest_price":       entry,
            "tp_hit":             0,
        }

        with self._lock:
            self.open_positions[trade_id] = trade

        logger.log_trade(
            "OPEN",
            trade_id=trade_id,
            asset=asset,
            direction=direction,
            entry=entry,
            size=pos_size,
            conf=confidence,
        )
        return trade

    # ── Update open positions with current prices ─────────────────────────────

    def update_positions(self, prices: Dict[str, float]) -> List[Dict]:
        """
        Check SL/TP for each position. Returns list of closed trades.
        prices = {asset: current_price}
        """
        closed = []
        with self._lock:
            for tid, pos in list(self.open_positions.items()):
                asset = pos.get("asset", "")
                price = prices.get(asset)
                if not price:
                    continue
                result = self._check_exit(pos, price)
                if result:
                    del self.open_positions[tid]
                    closed.append(result)

        for trade in closed:
            logger.log_trade(
                "CLOSE",
                trade_id=trade["trade_id"],
                asset=trade["asset"],
                pnl=round(trade["pnl"], 4),
                reason=trade.get("exit_reason", ""),
            )
            if self.on_trade_closed:
                try:
                    self.on_trade_closed(trade)
                except Exception as e:
                    logger.error(f"[PaperTrader] on_trade_closed error: {e}")

        return closed

    def _check_exit(self, pos: Dict, price: float) -> Optional[Dict]:
        direction  = pos.get("direction", pos.get("signal", "BUY"))
        entry      = float(pos.get("entry_price", 0))
        stop_loss  = float(pos.get("stop_loss", 0))
        take_profit= float(pos.get("take_profit", 0))
        tp_levels  = pos.get("take_profit_levels", [])
        size       = float(pos.get("position_size", 0))

        # Track extremes for trailing stop logic
        if direction == "BUY":
            pos["highest_price"] = max(float(pos.get("highest_price", entry)), price)
        else:
            pos["lowest_price"]  = min(float(pos.get("lowest_price", entry)), price)

        pnl = (price - entry) * size if direction == "BUY" else (entry - price) * size

        # ── Weekend market-closed guard ───────────────────────────────────────
        # Non-crypto markets (forex, commodities, indices) are closed on
        # Saturday all day and Sunday before 22:00 UTC.  SL and TP must not
        # trigger during this window — MT5 brokers hold the position open and
        # only execute when the market reopens.  Crypto is 24/7 so it is
        # always exempt from this guard.
        try:
            from datetime import datetime as _dt, timezone as _tz
            _category = pos.get("category", "forex")
            if _category != "crypto":
                _now  = _dt.now(tz=_tz.utc)
                _wd   = _now.weekday()   # 5=Sat 6=Sun
                _hour = _now.hour
                _weekend = (
                    _wd == 5                          # all Saturday
                    or (_wd == 6 and _hour < 22)      # Sunday before 22:00
                    or (_wd == 4 and _hour >= 22)     # Friday after 22:00
                )
                if _weekend:
                    pos["pnl"] = round(pnl, 6)
                    return None   # hold — market is closed
        except Exception:
            pass
        # ─────────────────────────────────────────────────────────────────────

        # ── Trailing stop + break-even ───────────────────────────────────────
        # Break-even at 60% toward TP — protects profit without cutting too early.
        # Trail at 90% toward TP with 0.5×ATR buffer — gives room to breathe on
        # 15m crypto candles which easily swing 0.3×ATR in one bar.
        if take_profit and entry and stop_loss:
            tp_dist = abs(take_profit - entry)
            sl_dist = abs(entry - stop_loss)
            if tp_dist > 0:
                if direction == "BUY":
                    progress = (price - entry) / tp_dist
                    if progress >= 0.90:
                        # Trail SL 0.5×ATR behind highest price reached
                        atr_approx = float(pos.get("original_sl", stop_loss))
                        atr_approx = abs(entry - atr_approx)  # original SL dist = 1×ATR
                        trail_sl = float(pos.get("highest_price", price)) - (0.5 * atr_approx)
                        if trail_sl > stop_loss:
                            pos["stop_loss"] = trail_sl
                            stop_loss = trail_sl
                    elif progress >= 0.60:
                        # Move SL to break-even — never lose on a trade that went 60% your way
                        if entry > stop_loss:
                            pos["stop_loss"] = entry
                            stop_loss = entry
                else:  # SELL
                    progress = (entry - price) / tp_dist
                    if progress >= 0.90:
                        atr_approx = float(pos.get("original_sl", stop_loss))
                        atr_approx = abs(entry - atr_approx)
                        trail_sl = float(pos.get("lowest_price", price)) + (0.5 * atr_approx)
                        if trail_sl < stop_loss:
                            pos["stop_loss"] = trail_sl
                            stop_loss = trail_sl
                    elif progress >= 0.60:
                        if entry < stop_loss:
                            pos["stop_loss"] = entry
                            stop_loss = entry
        # ─────────────────────────────────────────────────────────────────────

        # Stop loss
        if direction == "BUY"  and price <= stop_loss:
            reason = "Trailing Stop" if stop_loss != float(pos.get("original_sl", stop_loss)) else "Stop Loss"
            return self._close(pos, price, reason, pnl)
        if direction == "SELL" and price >= stop_loss:
            reason = "Trailing Stop" if stop_loss != float(pos.get("original_sl", stop_loss)) else "Stop Loss"
            return self._close(pos, price, reason, pnl)

        # Take profit levels (partial)
        if tp_levels:
            tp_idx = int(pos.get("tp_hit", 0))
            if tp_idx < len(tp_levels):
                tp_level = tp_levels[tp_idx]
                if direction == "BUY" and price >= tp_level:
                    pos["tp_hit"] = tp_idx + 1
                    if tp_idx + 1 >= len(tp_levels):
                        return self._close(pos, price, f"Take Profit {tp_idx + 1}", pnl)
                elif direction == "SELL" and price <= tp_level:
                    pos["tp_hit"] = tp_idx + 1
                    if tp_idx + 1 >= len(tp_levels):
                        return self._close(pos, price, f"Take Profit {tp_idx + 1}", pnl)
        elif take_profit:
            if direction == "BUY"  and price >= take_profit:
                return self._close(pos, price, "Take Profit", pnl)
            if direction == "SELL" and price <= take_profit:
                return self._close(pos, price, "Take Profit", pnl)

        # Update live PnL
        pos["pnl"] = round(pnl, 6)
        return None

    @staticmethod
    def _close(pos: Dict, exit_price: float, reason: str, pnl: float) -> Dict:
        entry     = float(pos.get("entry_price", exit_price))
        pnl_pct   = (pnl / (entry * float(pos.get("position_size", 1)))) * 100 if entry else 0.0
        open_time = pos.get("open_time", datetime.utcnow().isoformat())
        try:
            from datetime import datetime as dt
            duration = int((dt.utcnow() - dt.fromisoformat(open_time)).total_seconds() / 60)
        except Exception:
            duration = 0

        return {
            **pos,
            "exit_price":       exit_price,
            "exit_reason":      reason,
            "pnl":              round(pnl, 6),
            "pnl_percent":      round(pnl_pct, 4),
            "exit_time":        datetime.utcnow().isoformat(),
            "duration_minutes": duration,
        }

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict:
        with self._lock:
            return {
                "open_count":    len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "balance":        self.account_balance,
            }