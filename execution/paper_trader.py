from __future__ import annotations
import sys
import uuid
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
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
        account_balance: float = 10000.0,  # FIX: Changed from $30 to realistic balance
        risk_manager: Optional[RiskManager] = None,
    ):
        self.account_balance      = account_balance
        self._risk_manager        = risk_manager or RiskManager(account_balance)
        self.open_positions:  Dict[str, Dict] = {}
        self._lock                = threading.RLock()
        self.on_trade_closed: Optional[Callable[[Dict], None]] = None
        self.on_position_updated: Optional[Callable[[Dict], None]] = None

    # ── Restore persisted positions on restart ────────────────────────────────

    def restore_position(self, pos: Dict) -> None:
        tid = pos.get("trade_id")
        if tid:
            with self._lock:
                self.open_positions[tid] = pos

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _metadata_dict(raw: Any) -> Dict[str, Any]:
        return dict(raw) if isinstance(raw, dict) else {}

    def _resolve_execution_profile(self, category: str, metadata: Dict[str, Any]) -> Dict[str, float]:
        try:
            from config.config import get_backtest_execution_profile

            base = get_backtest_execution_profile(category)
        except Exception:
            base = {"commission": 0.001, "slippage": 0.0005, "risk_per_trade": 0.01}

        override = metadata.get("paper_execution_profile")
        if not isinstance(override, dict):
            override = {}

        structure = metadata.get("market_structure")
        if not isinstance(structure, dict):
            structure = {}

        spread_pct = self._safe_float(
            override.get("spread_pct"),
            self._safe_float(metadata.get("observed_spread_pct"), 0.0),
        )
        slippage = self._safe_float(
            override.get("slippage"),
            self._safe_float(base.get("slippage"), 0.0005),
        )
        commission = self._safe_float(
            override.get("commission"),
            self._safe_float(base.get("commission"), 0.001),
        )

        volatility_ratio = max(0.0, self._safe_float(metadata.get("volatility_ratio"), 1.0))
        volatility_state = str(
            metadata.get("volatility_state")
            or structure.get("volatility_state")
            or ""
        ).lower()
        volatility_multiplier = 1.0
        if volatility_ratio > 1.0:
            volatility_multiplier += min(0.40, (volatility_ratio - 1.0) * 0.30)
        if volatility_state == "expansion":
            volatility_multiplier += 0.08
        elif volatility_state == "extreme":
            volatility_multiplier += 0.18
        elif volatility_state == "compressed":
            volatility_multiplier -= 0.05

        micro = metadata.get("market_microstructure")
        if not isinstance(micro, dict):
            micro = {}
        stop_hunt_risk = self._safe_float(
            metadata.get("stop_hunt_risk"),
            self._safe_float(micro.get("stop_hunt_risk"), 0.0),
        )

        return {
            "commission": max(0.0, min(0.01, commission)),
            "slippage": max(0.0, min(0.01, slippage)),
            "spread_pct": max(0.0, min(0.02, spread_pct)),
            "volatility_multiplier": max(0.70, min(1.75, volatility_multiplier)),
            "stop_hunt_risk": max(0.0, min(1.0, stop_hunt_risk)),
        }

    @staticmethod
    def _apply_fill_price(price: float, side: str, slippage_pct: float, spread_pct: float) -> float:
        if price <= 0:
            return price
        total_pct = max(0.0, float(slippage_pct)) + max(0.0, float(spread_pct)) * 0.5
        if str(side).upper() == "BUY":
            return price * (1.0 + total_pct)
        return price * (1.0 - total_pct)

    def _fill_multiplier(self, metadata: Dict[str, Any], phase: str) -> float:
        phase_key = str(phase or "").lower()
        multiplier = 1.0
        if phase_key == "mark_to_market":
            multiplier = 0.25
        elif "stop" in phase_key:
            multiplier = 1.55
        elif "partial tp" in phase_key:
            multiplier = 0.55
        elif "take profit" in phase_key:
            multiplier = 0.70
        elif phase_key == "entry":
            multiplier = 1.0

        volatility_state = str(
            metadata.get("volatility_state")
            or (metadata.get("market_structure") or {}).get("volatility_state")
            or ""
        ).lower()
        if volatility_state == "expansion":
            multiplier += 0.08
        elif volatility_state == "extreme":
            multiplier += 0.18
        elif volatility_state == "compressed":
            multiplier -= 0.05

        micro = metadata.get("market_microstructure")
        if not isinstance(micro, dict):
            micro = {}
        stop_hunt_risk = self._safe_float(
            metadata.get("stop_hunt_risk"),
            self._safe_float(micro.get("stop_hunt_risk"), 0.0),
        )
        if "stop" in phase_key and stop_hunt_risk >= 0.45:
            multiplier += min(0.18, (stop_hunt_risk - 0.45) * 0.40)

        return max(0.15, min(1.85, multiplier))

    @staticmethod
    def _commission_cost(price: float, size: float, commission_rate: float) -> float:
        return abs(float(price or 0.0) * float(size or 0.0) * max(0.0, float(commission_rate or 0.0)))

    def _calculate_pnl(self, asset: str, category: str, entry: float, exit_price: float, size: float, direction: str) -> float:
        try:
            from risk.position_sizer import PositionSizer as _PS

            return _PS.pnl(asset, category, entry, exit_price, size, direction)
        except Exception:
            return (exit_price - entry) * size if direction == "BUY" else (entry - exit_price) * size

    def _calculate_net_pnl(
        self,
        pos: Dict,
        exit_price: float,
        size: float,
        commission_rate: float,
    ) -> Tuple[float, float, float, float, float]:
        asset = pos.get("asset", "")
        category = pos.get("category", "forex")
        direction = pos.get("direction", pos.get("signal", "BUY"))
        entry = float(pos.get("entry_price", exit_price))
        gross_pnl = self._calculate_pnl(asset, category, entry, exit_price, size, direction)
        entry_commission = self._commission_cost(entry, size, commission_rate)
        exit_commission = self._commission_cost(exit_price, size, commission_rate)
        total_commission = entry_commission + exit_commission
        net_pnl = gross_pnl - total_commission
        return gross_pnl, net_pnl, entry_commission, exit_commission, total_commission

    def _exit_fill(self, pos: Dict, reference_price: float, phase: str) -> Tuple[float, Dict[str, Any]]:
        metadata = self._metadata_dict(pos.get("metadata"))
        profile = self._resolve_execution_profile(str(pos.get("category", "forex")), metadata)
        direction = str(pos.get("direction", pos.get("signal", "BUY"))).upper()
        close_side = "SELL" if direction == "BUY" else "BUY"
        slippage_pct = profile["slippage"] * profile["volatility_multiplier"] * self._fill_multiplier(metadata, phase)
        fill_price = self._apply_fill_price(reference_price, close_side, slippage_pct, profile["spread_pct"])
        return fill_price, {
            "commission_rate": round(profile["commission"], 6),
            "base_slippage_pct": round(profile["slippage"], 6),
            "spread_pct": round(profile["spread_pct"], 6),
            "volatility_multiplier": round(profile["volatility_multiplier"], 4),
            "exit_side": close_side,
            "exit_slippage_pct": round(slippage_pct, 6),
            "exit_half_spread_pct": round(profile["spread_pct"] * 0.5, 6),
            "requested_exit_price": round(float(reference_price), 6),
            "exit_fill_price": round(float(fill_price), 6),
            "fill_mode": "paper_realistic",
        }

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
        metadata   = self._metadata_dict(signal.get("metadata"))

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

        # NOTE: Daily loss limit is checked in core/engine.py:_execute_signal()
        # before this method is called — no need to check again here

        execution_profile = self._resolve_execution_profile(category, metadata)
        entry_slippage_pct = (
            execution_profile["slippage"]
            * execution_profile["volatility_multiplier"]
            * self._fill_multiplier(metadata, "entry")
        )
        entry_side = "BUY" if direction == "BUY" else "SELL"
        filled_entry = self._apply_fill_price(
            entry,
            entry_side,
            entry_slippage_pct,
            execution_profile["spread_pct"],
        )
        paper_execution = {
            "fill_mode": "paper_realistic",
            "entry_side": entry_side,
            "requested_entry_price": round(entry, 6),
            "entry_fill_price": round(filled_entry, 6),
            "entry_slippage_pct": round(entry_slippage_pct, 6),
            "entry_half_spread_pct": round(execution_profile["spread_pct"] * 0.5, 6),
            "commission_rate": round(execution_profile["commission"], 6),
            "base_slippage_pct": round(execution_profile["slippage"], 6),
            "spread_pct": round(execution_profile["spread_pct"], 6),
            "volatility_multiplier": round(execution_profile["volatility_multiplier"], 4),
        }
        metadata = {**metadata, "paper_execution": paper_execution}

        risk_reward = float(signal.get("risk_reward", 0) or 0)
        try:
            risk = abs(filled_entry - stop_loss)
            reward = abs(take_profit - filled_entry)
            if risk > 0 and reward > 0:
                risk_reward = reward / risk
        except Exception:
            pass

        trade_id = str(uuid.uuid4())[:12]
        trade    = {
            "trade_id":           trade_id,
            "asset":              asset,
            "canonical_asset":    signal.get("canonical_asset", asset),
            "category":           category,
            "signal":             direction,
            "direction":          direction,
            "confidence":         round(confidence, 4),
            "entry_price":        filled_entry,
            "stop_loss":          stop_loss,
            "original_sl":        stop_loss,   # preserved for trailing stop detection
            "take_profit":        take_profit,
            "original_take_profit": take_profit,
            "take_profit_levels": tp_levels,
            "position_size":      pos_size,
            "strategy_id":        strategy,
            "open_time":          datetime.now(timezone.utc).isoformat(),
            "pnl":                0.0,
            "highest_price":      filled_entry,
            "lowest_price":       filled_entry,
            "tp_hit":             0,
            "risk_reward":        round(risk_reward, 4),
            "timestamp":          signal.get("timestamp"),
            "account_balance":    self.account_balance,
            "requested_entry_price": entry,
            "metadata":           metadata,
        }

        with self._lock:
            self.open_positions[trade_id] = trade

        logger.log_trade(
            "OPEN",
            trade_id=trade_id,
            asset=asset,
            direction=direction,
            entry=filled_entry,
            size=pos_size,
            score=round(confidence, 4),
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
        asset      = pos.get("asset", "")
        category   = pos.get("category", "forex")
        direction  = pos.get("direction", pos.get("signal", "BUY"))
        entry      = float(pos.get("entry_price", 0))
        stop_loss  = float(pos.get("stop_loss", 0))
        take_profit= float(pos.get("take_profit", 0))
        tp_levels  = pos.get("take_profit_levels", [])
        size       = float(pos.get("position_size", 0))
        metadata   = self._metadata_dict(pos.get("metadata"))
        tracked_before = {
            "position_size": float(pos.get("position_size", 0)),
            "stop_loss": float(pos.get("stop_loss", 0)),
            "tp_hit": int(pos.get("tp_hit", 0)),
            "highest_price": float(pos.get("highest_price", entry)),
            "lowest_price": float(pos.get("lowest_price", entry)),
        }

        # Track extremes for trailing stop logic
        if direction == "BUY":
            pos["highest_price"] = max(float(pos.get("highest_price", entry)), price)
        else:
            pos["lowest_price"]  = min(float(pos.get("lowest_price", entry)), price)

        mark_fill, mark_meta = self._exit_fill(pos, price, "mark_to_market")
        _, pnl, _, _, _ = self._calculate_net_pnl(
            pos,
            mark_fill,
            size,
            self._safe_float(mark_meta.get("commission_rate"), 0.0),
        )

        # ── Weekend market-closed guard ───────────────────────────────────────
        # Non-crypto markets (forex, commodities, indices) are closed on
        # Saturday all day and Sunday before 22:00 UTC.  SL and TP must not
        # trigger during this window - contract markets hold the position open and
        # only execute when the market reopens.  Crypto is 24/7 so it is
        # always exempt from this guard.
        try:
            from datetime import datetime as _dt, timezone as _tz
            if category != "crypto":
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
            close_fill, close_meta = self._exit_fill(pos, price, reason)
            gross_pnl, net_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                pos,
                close_fill,
                size,
                self._safe_float(close_meta.get("commission_rate"), 0.0),
            )
            return self._close(
                pos,
                close_fill,
                reason,
                net_pnl,
                {
                    "gross_pnl": round(gross_pnl, 6),
                    "entry_commission": round(entry_commission, 6),
                    "exit_commission": round(exit_commission, 6),
                    "total_commission": round(total_commission, 6),
                    "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), price), 6),
                    "metadata": {
                        "paper_execution": {
                            **self._metadata_dict(metadata.get("paper_execution")),
                            **close_meta,
                            "gross_pnl": round(gross_pnl, 6),
                            "net_pnl": round(net_pnl, 6),
                            "entry_commission": round(entry_commission, 6),
                            "exit_commission": round(exit_commission, 6),
                            "total_commission": round(total_commission, 6),
                            "execution_cost_drag": round(total_commission, 6),
                        }
                    },
                },
            )
        if direction == "SELL" and price >= stop_loss:
            reason = "Trailing Stop" if stop_loss != float(pos.get("original_sl", stop_loss)) else "Stop Loss"
            close_fill, close_meta = self._exit_fill(pos, price, reason)
            gross_pnl, net_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                pos,
                close_fill,
                size,
                self._safe_float(close_meta.get("commission_rate"), 0.0),
            )
            return self._close(
                pos,
                close_fill,
                reason,
                net_pnl,
                {
                    "gross_pnl": round(gross_pnl, 6),
                    "entry_commission": round(entry_commission, 6),
                    "exit_commission": round(exit_commission, 6),
                    "total_commission": round(total_commission, 6),
                    "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), price), 6),
                    "metadata": {
                        "paper_execution": {
                            **self._metadata_dict(metadata.get("paper_execution")),
                            **close_meta,
                            "gross_pnl": round(gross_pnl, 6),
                            "net_pnl": round(net_pnl, 6),
                            "entry_commission": round(entry_commission, 6),
                            "exit_commission": round(exit_commission, 6),
                            "total_commission": round(total_commission, 6),
                            "execution_cost_drag": round(total_commission, 6),
                        }
                    },
                },
            )

        # Take profit levels (partial)
        if tp_levels:
            tp_idx = int(pos.get("tp_hit", 0))
            if tp_idx < len(tp_levels):
                tp_level = tp_levels[tp_idx]
                hit = (direction == "BUY" and price >= tp_level) or \
                      (direction == "SELL" and price <= tp_level)
                if hit:
                    pos["tp_hit"] = tp_idx + 1
                    if tp_idx + 1 >= len(tp_levels):
                        # Final TP level — close full remaining position
                        close_fill, close_meta = self._exit_fill(pos, tp_level, f"Take Profit {tp_idx + 1}")
                        gross_pnl, net_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                            pos,
                            close_fill,
                            size,
                            self._safe_float(close_meta.get("commission_rate"), 0.0),
                        )
                        return self._close(
                            pos,
                            close_fill,
                            f"Take Profit {tp_idx + 1}",
                            net_pnl,
                            {
                                "gross_pnl": round(gross_pnl, 6),
                                "entry_commission": round(entry_commission, 6),
                                "exit_commission": round(exit_commission, 6),
                                "total_commission": round(total_commission, 6),
                                "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), tp_level), 6),
                                "metadata": {
                                    "paper_execution": {
                                        **self._metadata_dict(metadata.get("paper_execution")),
                                        **close_meta,
                                        "gross_pnl": round(gross_pnl, 6),
                                        "net_pnl": round(net_pnl, 6),
                                        "entry_commission": round(entry_commission, 6),
                                        "exit_commission": round(exit_commission, 6),
                                        "total_commission": round(total_commission, 6),
                                        "execution_cost_drag": round(total_commission, 6),
                                    }
                                },
                            },
                        )
                    else:
                        # FIX HIGH: Partial TP — close 1/3 of position and
                        # continue tracking the remainder.
                        # Previously tp_hit was incremented but position_size
                        # was never reduced — multi-TP was completely broken;
                        # only the final TP level ever closed anything.
                        total_tiers   = len(tp_levels)
                        close_fraction = 1.0 / (total_tiers - tp_idx)
                        original_size  = float(pos.get("position_size", size))
                        partial_size   = original_size * close_fraction
                        remaining_size = original_size - partial_size

                        close_fill, close_meta = self._exit_fill(pos, tp_level, f"Partial TP {tp_idx + 1}/{total_tiers}")
                        gross_partial_pnl, partial_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                            pos,
                            close_fill,
                            partial_size,
                            self._safe_float(close_meta.get("commission_rate"), 0.0),
                        )

                        # Build a partial-close trade record
                        parent_trade_id = str(pos.get("trade_id", ""))
                        partial_trade = self._close(
                            dict(
                                pos,
                                trade_id=f"{parent_trade_id}-PT{tp_idx + 1}",
                                parent_trade_id=parent_trade_id,
                                is_partial_close=True,
                                position_size=partial_size,
                            ),
                            close_fill,
                            f"Partial TP {tp_idx + 1}/{total_tiers}",
                            partial_pnl,
                            {
                                "gross_pnl": round(gross_partial_pnl, 6),
                                "entry_commission": round(entry_commission, 6),
                                "exit_commission": round(exit_commission, 6),
                                "total_commission": round(total_commission, 6),
                                "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), tp_level), 6),
                                "metadata": {
                                    "paper_execution": {
                                        **self._metadata_dict(metadata.get("paper_execution")),
                                        **close_meta,
                                        "gross_pnl": round(gross_partial_pnl, 6),
                                        "net_pnl": round(partial_pnl, 6),
                                        "entry_commission": round(entry_commission, 6),
                                        "exit_commission": round(exit_commission, 6),
                                        "total_commission": round(total_commission, 6),
                                        "execution_cost_drag": round(total_commission, 6),
                                    }
                                },
                            },
                        )

                        # Reduce remaining position size and update SL to break-even
                        pos["position_size"] = remaining_size
                        if direction == "BUY" and entry > float(pos.get("stop_loss", 0)):
                            pos["stop_loss"] = entry   # lock in break-even
                        elif direction == "SELL" and entry < float(pos.get("stop_loss", 99e9)):
                            pos["stop_loss"] = entry

                        self._notify_position_updated(pos)

                        # Fire the callback for the partial close
                        if partial_trade and self.on_trade_closed:
                            try:
                                self.on_trade_closed(partial_trade)
                            except Exception as _e:
                                logger.error(f"[PaperTrader] partial TP callback error: {_e}")
                        return None   # position still open (remainder)
        elif take_profit:
            if direction == "BUY"  and price >= take_profit:
                close_fill, close_meta = self._exit_fill(pos, take_profit, "Take Profit")
                gross_pnl, net_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                    pos,
                    close_fill,
                    size,
                    self._safe_float(close_meta.get("commission_rate"), 0.0),
                )
                return self._close(
                    pos,
                    close_fill,
                    "Take Profit",
                    net_pnl,
                    {
                        "gross_pnl": round(gross_pnl, 6),
                        "entry_commission": round(entry_commission, 6),
                        "exit_commission": round(exit_commission, 6),
                        "total_commission": round(total_commission, 6),
                        "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), take_profit), 6),
                        "metadata": {
                            "paper_execution": {
                                **self._metadata_dict(metadata.get("paper_execution")),
                                **close_meta,
                                "gross_pnl": round(gross_pnl, 6),
                                "net_pnl": round(net_pnl, 6),
                                "entry_commission": round(entry_commission, 6),
                                "exit_commission": round(exit_commission, 6),
                                "total_commission": round(total_commission, 6),
                                "execution_cost_drag": round(total_commission, 6),
                            }
                        },
                    },
                )
            if direction == "SELL" and price <= take_profit:
                close_fill, close_meta = self._exit_fill(pos, take_profit, "Take Profit")
                gross_pnl, net_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
                    pos,
                    close_fill,
                    size,
                    self._safe_float(close_meta.get("commission_rate"), 0.0),
                )
                return self._close(
                    pos,
                    close_fill,
                    "Take Profit",
                    net_pnl,
                    {
                        "gross_pnl": round(gross_pnl, 6),
                        "entry_commission": round(entry_commission, 6),
                        "exit_commission": round(exit_commission, 6),
                        "total_commission": round(total_commission, 6),
                        "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), take_profit), 6),
                        "metadata": {
                            "paper_execution": {
                                **self._metadata_dict(metadata.get("paper_execution")),
                                **close_meta,
                                "gross_pnl": round(gross_pnl, 6),
                                "net_pnl": round(net_pnl, 6),
                                "entry_commission": round(entry_commission, 6),
                                "exit_commission": round(exit_commission, 6),
                                "total_commission": round(total_commission, 6),
                                "execution_cost_drag": round(total_commission, 6),
                            }
                        },
                    },
                )

        # Update live PnL
        pos["pnl"] = round(pnl, 6)
        tracked_after = {
            "position_size": float(pos.get("position_size", 0)),
            "stop_loss": float(pos.get("stop_loss", 0)),
            "tp_hit": int(pos.get("tp_hit", 0)),
            "highest_price": float(pos.get("highest_price", entry)),
            "lowest_price": float(pos.get("lowest_price", entry)),
        }
        if tracked_after != tracked_before:
            self._notify_position_updated(pos)
        return None

    def _notify_position_updated(self, pos: Dict) -> None:
        if not self.on_position_updated:
            return
        try:
            self.on_position_updated(dict(pos))
        except Exception as e:
            logger.error(f"[PaperTrader] on_position_updated error: {e}")

    @staticmethod
    def _close(pos: Dict, exit_price: float, reason: str, pnl: float, details: Optional[Dict[str, Any]] = None) -> Dict:
        entry     = float(pos.get("entry_price", exit_price))
        # FIX HIGH: pnl_pct previously always used pos.get("balance", 10000)
        # fallback.  The balance is almost never stored in the position dict,
        # so every trade showed pnl_pct relative to a fictional $10,000 account
        # — on a $30 account a $1 profit appeared as 0.01% instead of 3.3%.
        # Now we read from SystemState when available, falling back gracefully.
        try:
            _eng_mod = sys.modules.get("core.engine")
            _state = getattr(getattr(_eng_mod, "_CORE_INSTANCE", None), "state", None) if _eng_mod is not None else None
            real_balance = float(_state.balance) if _state else None
        except Exception:
            real_balance = None
        approx_balance = (
            real_balance
            or float(pos.get("balance", 0))
            or float(pos.get("account_balance", 0))
            or 10_000.0   # last-resort fallback
        )
        pnl_pct = (pnl / approx_balance) * 100 if approx_balance else 0.0
        open_time = pos.get("open_time", datetime.now(timezone.utc).isoformat())
        exit_time = datetime.now(timezone.utc).isoformat()
        
        # Calculate duration from open_time to exit_time (in minutes)
        try:
            from datetime import datetime as dt_class
            open_time_str = pos.get("open_time", "")
            exit_time = datetime.now(timezone.utc).isoformat()
            if open_time_str:
                try:
                    open_dt = dt_class.fromisoformat(open_time_str)
                except (ValueError, TypeError):
                    # If that fails, try removing 'Z' suffix and retry
                    if open_time_str.endswith('Z'):
                        open_dt = dt_class.fromisoformat(open_time_str[:-1])
                    else:
                        raise
                
                exit_dt = dt_class.fromisoformat(exit_time)
                duration_seconds = (exit_dt - open_dt).total_seconds()
                duration = int(duration_seconds / 60)
                if duration < 0:
                    duration = 0
            else:
                duration = 0
        except Exception:
            duration = 0

        snapshot = {
            **pos,
            "exit_price":       exit_price,
            "exit_reason":      reason,
            "pnl":              round(pnl, 6),
            "pnl_percent":      round(pnl_pct, 4),
            "exit_time":        exit_time,
            "duration_minutes": duration,
        }
        if isinstance(details, dict):
            details_meta = details.get("metadata")
            for key, value in details.items():
                if key == "metadata":
                    continue
                snapshot[key] = value
            if isinstance(details_meta, dict):
                metadata = dict(snapshot.get("metadata") or {})
                metadata.update(details_meta)
                snapshot["metadata"] = metadata
        return snapshot

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> Dict:
        with self._lock:
            return {
                "open_count":    len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "balance":        self.account_balance,
            }
