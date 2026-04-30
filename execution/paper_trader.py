from __future__ import annotations
import sys
import uuid
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple, cast
from risk.manager import RiskManager
from utils.logger import get_logger

logger = get_logger()
TradeDict = Dict[str, Any]


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
        self.open_positions: Dict[str, TradeDict] = {}
        self._lock                = threading.RLock()
        self.on_trade_closed: Optional[Callable[[TradeDict], None]] = None
        self.on_position_updated: Optional[Callable[[TradeDict], None]] = None

    # ── Restore persisted positions on restart ────────────────────────────────

    def restore_position(self, pos: TradeDict) -> None:
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
        return cast(Dict[str, Any], dict(raw) if isinstance(raw, dict) else {})

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
    def _commission_notional_usd(asset: str, category: str, price: float, size: float) -> float:
        category = str(category or "").lower()
        symbol = str(asset or "").upper()
        price = abs(float(price or 0.0))
        size = abs(float(size or 0.0))
        if size <= 0.0:
            return 0.0

        if category == "forex" and "/" in symbol:
            base, quote = symbol.split("/", 1)
            base = base.strip().upper()
            quote = quote.strip().upper()
            if quote == "USD":
                return price * size
            if base == "USD":
                return size
            # Crosses like EUR/JPY or GBP/JPY do not have a direct USD quote in
            # this context; using base-units notional is a closer approximation
            # than multiplying by the JPY quote and overstating commission.
            return size

        return price * size

    @classmethod
    def _commission_cost(cls, asset: str, category: str, price: float, size: float, commission_rate: float) -> float:
        notional_usd = cls._commission_notional_usd(asset, category, price, size)
        return abs(notional_usd * max(0.0, float(commission_rate or 0.0)))

    def _calculate_pnl(self, asset: str, category: str, entry: float, exit_price: float, size: float, direction: str) -> float:
        try:
            from risk.position_sizer import PositionSizer as _PS

            return _PS.pnl(asset, category, entry, exit_price, size, direction)
        except Exception:
            return (exit_price - entry) * size if direction == "BUY" else (entry - exit_price) * size

    def _calculate_net_pnl(
        self,
        pos: TradeDict,
        exit_price: float,
        size: float,
        commission_rate: float,
    ) -> Tuple[float, float, float, float, float]:
        asset = pos.get("asset", "")
        category = pos.get("category", "forex")
        direction = pos.get("direction", pos.get("signal", "BUY"))
        entry = float(pos.get("entry_price", exit_price))
        gross_pnl = self._calculate_pnl(asset, category, entry, exit_price, size, direction)
        entry_commission = self._commission_cost(asset, category, entry, size, commission_rate)
        exit_commission = self._commission_cost(asset, category, exit_price, size, commission_rate)
        total_commission = entry_commission + exit_commission
        net_pnl = gross_pnl - total_commission
        return gross_pnl, net_pnl, entry_commission, exit_commission, total_commission

    def _exit_fill(self, pos: TradeDict, reference_price: float, phase: str) -> Tuple[float, Dict[str, Any]]:
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

    def execute_signal(self, signal: TradeDict) -> Optional[TradeDict]:
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
        raw_tp_levels = signal.get("take_profit_levels", [])
        tp_levels: List[float] = []
        for raw_level in list(raw_tp_levels or []):
            try:
                level = float(raw_level)
            except Exception:
                continue
            if level > 0:
                tp_levels.append(round(level, 6))
        category   = signal.get("category", "forex")
        strategy   = signal.get("strategy_id", "UNKNOWN")
        metadata   = self._metadata_dict(signal.get("metadata"))

        if tp_levels:
            take_profit = float(tp_levels[-1])
            metadata = {
                **metadata,
                "primary_take_profit": float(tp_levels[0]),
                "runner_take_profit": float(tp_levels[-1]),
            }

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

        try:
            from risk.position_sizer import PositionSizer as _PS

            lot_size = _PS.lots_from_size(asset, category, pos_size)
        except Exception:
            lot_size = 0.0

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
            "initial_position_size": pos_size,
            "lot_size":           round(lot_size, 4),
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
        trade["management_checkpoint_at"] = trade["open_time"]

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

    @staticmethod
    def _tp_size_shares(management: Dict[str, Any], total_tiers: int) -> List[float]:
        if total_tiers <= 0:
            return []
        parsed: List[float] = []
        for raw in list(management.get("partial_take_profit_size_fractions") or []):
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

    # ── Update open positions with current prices ─────────────────────────────

    def update_positions(self, prices: Dict[str, float]) -> List[TradeDict]:
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

    def _check_exit(self, pos: TradeDict, price: float) -> Optional[TradeDict]:
        asset = pos.get("asset", "")
        category = pos.get("category", "forex")
        direction = pos.get("direction", pos.get("signal", "BUY"))
        entry = float(pos.get("entry_price", 0))
        stop_loss = float(pos.get("stop_loss", 0))
        take_profit = float(pos.get("take_profit", 0))
        tp_levels = pos.get("take_profit_levels", [])
        size = float(pos.get("position_size", 0))
        metadata = self._metadata_dict(pos.get("metadata"))
        management = self._metadata_dict(metadata.get("trade_management_plan"))
        tracked_before = {
            "position_size": float(pos.get("position_size", 0)),
            "stop_loss": float(pos.get("stop_loss", 0)),
            "tp_hit": int(pos.get("tp_hit", 0)),
            "highest_price": float(pos.get("highest_price", entry)),
            "lowest_price": float(pos.get("lowest_price", entry)),
        }

        self._update_exit_extremes(pos, price, direction, entry)
        pnl, _ = self._mark_to_market_pnl(pos, price, size)

        if self._is_weekend_market_closed(category):
            pos["pnl"] = round(pnl, 6)
            return None

        stop_loss = self._apply_trailing_stop_rules(
            pos,
            price,
            direction,
            entry,
            stop_loss,
            take_profit,
            metadata,
            management,
        )

        stop_loss_result = self._close_on_stop_loss(pos, price, direction, stop_loss, size, metadata)
        if stop_loss_result is not None:
            return stop_loss_result

        take_profit_result = self._process_take_profit_targets(
            pos,
            price,
            direction,
            entry,
            take_profit,
            tp_levels,
            size,
            metadata,
            management,
        )
        if take_profit_result is not None:
            return take_profit_result

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

    def _update_exit_extremes(self, pos: TradeDict, price: float, direction: str, entry: float) -> None:
        if direction == "BUY":
            pos["highest_price"] = max(float(pos.get("highest_price", entry)), price)
        else:
            pos["lowest_price"] = min(float(pos.get("lowest_price", entry)), price)

    def _mark_to_market_pnl(self, pos: TradeDict, price: float, size: float) -> tuple[float, Dict[str, Any]]:
        mark_fill, mark_meta = self._exit_fill(pos, price, "mark_to_market")
        asset = pos.get("asset", "")
        category = pos.get("category", "forex")
        direction = pos.get("direction", pos.get("signal", "BUY"))
        entry = float(pos.get("entry_price", price))
        pnl = self._calculate_pnl(asset, category, entry, mark_fill, size, direction)
        return pnl, mark_meta

    @staticmethod
    def _is_weekend_market_closed(category: str) -> bool:
        if category == "crypto":
            return False
        try:
            from datetime import datetime as _dt, timezone as _tz

            now = _dt.now(tz=_tz.utc)
            weekday = now.weekday()
            hour = now.hour
            return weekday == 5 or (weekday == 6 and hour < 22) or (weekday == 4 and hour >= 22)
        except Exception:
            return False

    def _apply_trailing_stop_rules(
        self,
        pos: TradeDict,
        price: float,
        direction: str,
        entry: float,
        stop_loss: float,
        take_profit: float,
        metadata: Dict[str, Any],
        management: Dict[str, Any],
    ) -> float:
        if not entry or not stop_loss:
            return stop_loss

        initial_risk = abs(entry - float(pos.get("original_sl", stop_loss)))
        if management and initial_risk > 0.0:
            return self._apply_management_trailing_rules(
                pos,
                price,
                direction,
                entry,
                stop_loss,
                metadata,
                management,
                initial_risk,
            )
        if take_profit:
            return self._apply_fallback_trailing_rules(pos, price, direction, entry, stop_loss, take_profit)
        return stop_loss

    def _apply_management_trailing_rules(
        self,
        pos: TradeDict,
        price: float,
        direction: str,
        entry: float,
        stop_loss: float,
        metadata: Dict[str, Any],
        management: Dict[str, Any],
        initial_risk: float,
    ) -> float:
        atr_value = self._safe_float(metadata.get("atr"), 0.0)
        trail_activation_rr = max(0.5, self._safe_float(management.get("trail_activation_rr"), 1.0))
        trail_atr_multiple = max(0.4, self._safe_float(management.get("trail_atr_multiple"), 0.8))
        favorable_move = (price - entry) if direction == "BUY" else (entry - price)
        progress_rr = favorable_move / max(initial_risk, 1e-9)
        if progress_rr >= trail_activation_rr:
            trail_dist = max(initial_risk * 0.85, atr_value * trail_atr_multiple if atr_value > 0 else 0.0)
            if direction == "BUY":
                trail_sl = float(pos.get("highest_price", price)) - trail_dist
                if trail_sl > stop_loss:
                    pos["stop_loss"] = trail_sl
                    stop_loss = trail_sl
            else:
                trail_sl = float(pos.get("lowest_price", price)) + trail_dist
                if trail_sl < stop_loss:
                    pos["stop_loss"] = trail_sl
                    stop_loss = trail_sl
        return stop_loss

    def _apply_fallback_trailing_rules(
        self,
        pos: TradeDict,
        price: float,
        direction: str,
        entry: float,
        stop_loss: float,
        take_profit: float,
    ) -> float:
        tp_dist = abs(take_profit - entry)
        if tp_dist <= 0:
            return stop_loss

        initial_risk = abs(entry - float(pos.get("original_sl", stop_loss)))
        if initial_risk <= 0:
            return stop_loss

        progress = ((price - entry) / tp_dist) if direction == "BUY" else ((entry - price) / tp_dist)
        favorable_move = ((price - entry) if direction == "BUY" else (entry - price))
        progress_rr = favorable_move / max(initial_risk, 1e-9)
        partials_taken = int(pos.get("tp_hit", 0) or 0)
        break_even_buffer = initial_risk * (0.12 if partials_taken > 0 else 0.05)

        if direction == "BUY":
            if progress >= 0.95 or progress_rr >= 1.75:
                trail_sl = float(pos.get("highest_price", price)) - (0.90 * initial_risk)
                if partials_taken > 0:
                    trail_sl = max(trail_sl, entry + break_even_buffer)
                if trail_sl > stop_loss:
                    pos["stop_loss"] = trail_sl
                    stop_loss = trail_sl
            elif progress >= 0.78 or progress_rr >= 1.15:
                buffered_be = entry + break_even_buffer
                if buffered_be > stop_loss:
                    pos["stop_loss"] = buffered_be
                    stop_loss = buffered_be
        else:
            if progress >= 0.95 or progress_rr >= 1.75:
                trail_sl = float(pos.get("lowest_price", price)) + (0.90 * initial_risk)
                if partials_taken > 0:
                    trail_sl = min(trail_sl, entry - break_even_buffer)
                if trail_sl < stop_loss:
                    pos["stop_loss"] = trail_sl
                    stop_loss = trail_sl
            elif progress >= 0.78 or progress_rr >= 1.15:
                buffered_be = entry - break_even_buffer
                if buffered_be < stop_loss:
                    pos["stop_loss"] = buffered_be
                    stop_loss = buffered_be
        return stop_loss

    @staticmethod
    def _is_stop_loss_hit(direction: str, price: float, stop_loss: float) -> bool:
        if direction == "BUY":
            return price <= stop_loss
        return price >= stop_loss

    @staticmethod
    def _is_price_beyond_level(direction: str, price: float, level: float) -> bool:
        if direction == "BUY":
            return price >= level
        return price <= level

    @staticmethod
    def _closest_level_to_price(direction: str, price: float, levels: List[float]) -> float:
        valid_levels = [float(level) for level in levels if float(level) > 0]
        if not valid_levels:
            return float(price)
        if direction == "BUY":
            crossed = [level for level in valid_levels if level <= price]
            return max(crossed) if crossed else min(valid_levels)
        crossed = [level for level in valid_levels if level >= price]
        return min(crossed) if crossed else max(valid_levels)

    @staticmethod
    def _is_take_profit_hit(direction: str, price: float, take_profit: float) -> bool:
        if direction == "BUY":
            return price >= take_profit
        return price <= take_profit

    def _build_close_details(
        self,
        metadata: Dict[str, Any],
        close_meta: Dict[str, Any],
        gross_pnl: float,
        net_pnl: float,
        entry_commission: float,
        exit_commission: float,
        total_commission: float,
        *,
        fallback_exit_price: float,
    ) -> Dict[str, Any]:
        return {
            "gross_pnl": round(gross_pnl, 6),
            "entry_commission": round(entry_commission, 6),
            "exit_commission": round(exit_commission, 6),
            "total_commission": round(total_commission, 6),
            "requested_exit_price": round(self._safe_float(close_meta.get("requested_exit_price"), fallback_exit_price), 6),
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
        }

    def _close_trade(
        self,
        pos: TradeDict,
        exit_price: float,
        reason: str,
        size: float,
        metadata: Dict[str, Any],
    ) -> TradeDict:
        close_fill, close_meta = self._exit_fill(pos, exit_price, reason)
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
            self._build_close_details(
                metadata,
                close_meta,
                gross_pnl,
                net_pnl,
                entry_commission,
                exit_commission,
                total_commission,
                fallback_exit_price=exit_price,
            ),
        )

    def _close_on_stop_loss(
        self,
        pos: TradeDict,
        price: float,
        direction: str,
        stop_loss: float,
        size: float,
        metadata: Dict[str, Any],
    ) -> Optional[TradeDict]:
        if self._is_stop_loss_hit(direction, price, stop_loss):
            reason = "Trailing Stop" if stop_loss != float(pos.get("original_sl", stop_loss)) else "Stop Loss"
            return self._close_trade(pos, stop_loss, reason, size, metadata)
        return None

    def _close_partial_take_profit(
        self,
        pos: TradeDict,
        tp_level: float,
        tp_idx: int,
        total_tiers: int,
        partial_size: float,
        metadata: Dict[str, Any],
    ) -> TradeDict:
        close_fill, close_meta = self._exit_fill(pos, tp_level, f"Partial TP {tp_idx + 1}/{total_tiers}")
        gross_partial_pnl, partial_pnl, entry_commission, exit_commission, total_commission = self._calculate_net_pnl(
            pos,
            close_fill,
            partial_size,
            self._safe_float(close_meta.get("commission_rate"), 0.0),
        )

        parent_trade_id = str(pos.get("trade_id", ""))
        return self._close(
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
            self._build_close_details(
                metadata,
                close_meta,
                gross_partial_pnl,
                partial_pnl,
                entry_commission,
                exit_commission,
                total_commission,
                fallback_exit_price=tp_level,
            ),
        )

    def _protect_runner_after_partial(
        self,
        pos: TradeDict,
        *,
        price: float,
        direction: str,
        entry: float,
        take_profit: float,
        metadata: Dict[str, Any],
        management: Dict[str, Any],
    ) -> None:
        current_stop = float(pos.get("stop_loss", entry) or entry)
        original_stop = float(pos.get("original_sl", current_stop) or current_stop)
        initial_risk = abs(entry - original_stop)
        if initial_risk <= 0.0:
            return

        if bool(management.get("break_even_after_partial", False)):
            if direction == "BUY":
                pos["stop_loss"] = max(current_stop, entry)
            else:
                pos["stop_loss"] = min(current_stop, entry)

        protected_stop = float(pos.get("stop_loss", current_stop) or current_stop)
        if management:
            self._apply_management_trailing_rules(
                pos,
                price,
                direction,
                entry,
                protected_stop,
                metadata,
                management,
                initial_risk,
            )
            return

        self._apply_fallback_trailing_rules(
            pos,
            price,
            direction,
            entry,
            protected_stop,
            take_profit,
        )

    def _process_take_profit_targets(
        self,
        pos: TradeDict,
        price: float,
        direction: str,
        entry: float,
        take_profit: float,
        tp_levels: List[float],
        size: float,
        metadata: Dict[str, Any],
        management: Dict[str, Any],
    ) -> Optional[TradeDict]:
        if tp_levels:
            total_tiers = len(tp_levels)
            size_shares = self._tp_size_shares(management, total_tiers)
            while True:
                tp_idx = int(pos.get("tp_hit", 0))
                if tp_idx >= total_tiers:
                    break

                tp_level = float(tp_levels[tp_idx])
                if not self._is_take_profit_hit(direction, price, tp_level):
                    break

                pos["tp_hit"] = tp_idx + 1
                if tp_idx + 1 >= total_tiers:
                    return self._close_trade(
                        pos,
                        self._closest_level_to_price(direction, price, tp_levels),
                        f"Take Profit {tp_idx + 1}",
                        float(pos.get("position_size", size)),
                        metadata,
                    )

                original_size = float(pos.get("position_size", size))
                initial_size = float(pos.get("initial_position_size", size) or size)
                target_share = float(size_shares[tp_idx]) if tp_idx < len(size_shares) else (1.0 / max(2, total_tiers - tp_idx))
                partial_size = min(original_size, initial_size * target_share)
                remaining_size = max(0.0, original_size - partial_size)
                partial_trade = self._close_partial_take_profit(
                    pos,
                    tp_level,
                    tp_idx,
                    total_tiers,
                    partial_size,
                    metadata,
                )

                pos["position_size"] = remaining_size
                self._protect_runner_after_partial(
                    pos,
                    price=price,
                    direction=direction,
                    entry=entry,
                    take_profit=take_profit,
                    metadata=metadata,
                    management=management,
                )

                self._notify_position_updated(pos)
                if partial_trade and self.on_trade_closed:
                    try:
                        self.on_trade_closed(partial_trade)
                    except Exception as _e:
                        logger.error(f"[PaperTrader] partial TP callback error: {_e}")

                if remaining_size <= 0:
                    return self._close_trade(pos, tp_level, f"Take Profit {tp_idx + 1}", remaining_size, metadata)

        elif take_profit and self._is_take_profit_hit(direction, price, take_profit):
            return self._close_trade(pos, take_profit, "Take Profit", size, metadata)
        return None
    def _notify_position_updated(self, pos: TradeDict) -> None:
        if not self.on_position_updated:
            return
        try:
            checkpoint_at = datetime.now(timezone.utc).isoformat()
            pos["management_checkpoint_at"] = checkpoint_at
            snapshot = dict(pos)
            snapshot["management_checkpoint_at"] = checkpoint_at
            self.on_position_updated(snapshot)
        except Exception as e:
            logger.error(f"[PaperTrader] on_position_updated error: {e}")

    @staticmethod
    def _close(pos: TradeDict, exit_price: float, reason: str, pnl: float, details: Optional[Dict[str, Any]] = None) -> TradeDict:
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

    def get_stats(self) -> TradeDict:
        with self._lock:
            return {
                "open_count":    len(self.open_positions),
                "open_positions": list(self.open_positions.values()),
                "balance":        self.account_balance,
            }
