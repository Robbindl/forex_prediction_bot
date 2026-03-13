"""
core/engine.py — TradingCore: the single central trading engine.

This is the ONE object that owns all trading logic.
No other module creates its own engine, trading loop, or trading state.

Architecture:
    TradingCore holds:
      • The UltimateTradingSystem instance (scanner, predictor, risk, fetcher)
      • SystemState (single source of truth for all mutable state)
      • EventBus (typed events for all subsystems to subscribe to)
      • AssetRegistry (canonical asset identity)

    External modules interact with TradingCore via its public API:
      • get_positions()         → list of open position dicts
      • get_performance()       → performance metrics dict
      • get_balance()           → current balance
      • get_signals(asset)      → run the 7-layer pipeline for one asset
      • close_position(tid)     → manual close
      • set_cooldown(asset, m)  → manual cooldown
      • subscribe(event, cb)    → subscribe to events

    The trading loop runs INSIDE TradingCore on a daemon thread.
    Web dashboard and Telegram connect to this instance directly.
    There is no subprocess IPC — everything is in-process.

Usage (from bot.py):
    from core.engine import TradingCore
    engine = TradingCore(balance=500, strategy_mode='voting')
    engine.start()
    # Flask app, Telegram commander etc. receive engine as an argument
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type

from logger import logger
from core.events import (
    EventBus, BaseEvent,
    SystemStartedEvent, SystemStoppingEvent,
    TradeOpenedEvent, TradeClosedEvent,
    SignalGeneratedEvent, SignalRejectedEvent,
    RiskLimitHitEvent, PositionUpdateEvent,
    BalanceChangedEvent, CooldownActivatedEvent,
    HealthCheckEvent,
)
from core.state import SystemState
from core.assets import AssetRegistry


class TradingCore:
    """
    Central trading engine — single instance, single source of truth.

    All subsystems (web dashboard, Telegram, performance analytics,
    monitoring, auto-trainer) interact with this object.
    """

    def __init__(
        self,
        balance: float = 30.0,
        strategy_mode: str = "voting",
        no_telegram: bool = False,
    ):
        self.balance       = balance
        self.strategy_mode = strategy_mode
        self.no_telegram   = no_telegram

        # ── Core singletons ───────────────────────────────────────────────
        self.events   = EventBus()
        self.state    = SystemState()
        self.registry = AssetRegistry()

        # Update balance from persisted state or use provided value
        if self.state.open_position_count() == 0:
            # Fresh start — use provided balance
            self.state.set_balance(balance, "startup")
        else:
            # Positions restored from disk — use persisted balance
            logger.info(
                f"[TradingCore] Restored balance ${self.state.balance:.2f} "
                f"and {self.state.open_position_count()} open positions"
            )

        # ── UltimateTradingSystem (the actual scanner/predictor/trader) ───
        self._engine: Optional[Any] = None   # set in _init_engine()
        self._engine_ready = threading.Event()
        self._is_running   = False
        self._stop_event   = threading.Event()
        self._loop_thread: Optional[threading.Thread] = None

        # ── Wired subsystems (set by callers after construction) ──────────
        self.telegram: Optional[Any]       = None
        self.monitor:  Optional[Any]       = None

        # ── Event wiring: internal handlers ──────────────────────────────
        self._wire_internal_events()

        logger.info(
            f"[TradingCore] Initialized — "
            f"balance=${balance} strategy={strategy_mode}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Startup / shutdown
    # ─────────────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Start the trading engine in a background daemon thread.
        Returns immediately — use engine_ready event to wait for init.
        """
        if self._is_running:
            logger.warning("[TradingCore] Already running")
            return

        self._is_running = True
        self._stop_event.clear()

        self._loop_thread = threading.Thread(
            target=self._run,
            name="TradingCore-loop",
            daemon=True,
        )
        self._loop_thread.start()
        logger.info("[TradingCore] Trading loop started")

    def stop(self, reason: str = "manual") -> None:
        """Gracefully stop the engine. Saves state before exit."""
        if not self._is_running:
            return

        logger.info(f"[TradingCore] Stopping — reason: {reason}")
        n_open = self.state.open_position_count()

        self.events.emit(SystemStoppingEvent(
            reason=reason,
            open_positions_count=n_open,
        ))

        self._stop_event.set()
        self._is_running = False

        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)

        self.state.force_save()
        logger.info("[TradingCore] Stopped and state saved")

    def wait_until_ready(self, timeout: float = 60.0) -> bool:
        """Block until the engine finishes initializing. Returns True if ready."""
        return self._engine_ready.wait(timeout=timeout)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_ready(self) -> bool:
        return self._engine_ready.is_set()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — positions
    # ─────────────────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Dict]:
        """Return all open positions."""
        return self.state.get_open_positions()

    def get_closed_trades(self, limit: int = 100) -> List[Dict]:
        return self.state.get_closed_positions(limit=limit)

    def close_position_manually(self, trade_id: str) -> Optional[Dict]:
        """
        Manually close an open position at current market price.
        Fires TradeClosed event.
        """
        if not self._engine:
            return None
        try:
            pos = self.state.get_open_position(trade_id)
            if not pos:
                return None

            asset    = pos.get("asset", "")
            category = pos.get("category", "unknown")

            # Fetch current price
            current_price = None
            try:
                current_price, _ = self._engine.fetcher.get_real_time_price(
                    asset, category
                )
            except Exception:
                pass

            if not current_price:
                current_price = pos.get("entry_price", 0)

            # Calculate P&L
            direction = pos.get("signal", "BUY")
            size      = pos.get("position_size", 0)
            entry     = pos.get("entry_price", 0)
            if direction == "BUY":
                pnl = (current_price - entry) * size
            else:
                pnl = (entry - current_price) * size

            closed = self.state.close_position(
                trade_id, current_price, "Manual Close", pnl
            )
            if closed:
                self._emit_trade_closed(closed)
            return closed

        except Exception as e:
            logger.error(f"[TradingCore] Manual close failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — balance & performance
    # ─────────────────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        return self.state.balance

    def get_performance(self) -> Dict:
        return self.state.get_performance()

    def get_daily_stats(self) -> Dict:
        return {
            "daily_trades": self.state.daily_trades,
            "daily_pnl":    self.state.daily_pnl,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — signals (on-demand, for Telegram /signal command)
    # ─────────────────────────────────────────────────────────────────────────

    def get_signal_for_asset(self, asset: str) -> Optional[Dict]:
        """
        Run the full 7-layer pipeline for a single asset on demand.
        Used by TelegramCommander /signal command.
        Returns signal dict or None.
        """
        if not self.is_ready or not self._engine:
            return None
        canonical = self.registry.canonical(asset)
        category  = self.registry.category(canonical)
        try:
            from signal_learning import get_instant_signal
            return get_instant_signal(canonical, category, self._engine)
        except Exception as e:
            logger.error(f"[TradingCore] get_signal_for_asset({asset}): {e}")
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — cooldowns
    # ─────────────────────────────────────────────────────────────────────────

    def set_cooldown(self, asset: str, minutes: int = 60) -> None:
        canonical = self.registry.canonical(asset)
        self.state.set_cooldown(canonical, minutes)
        self.events.emit(CooldownActivatedEvent(
            asset=asset,
            canonical_asset=canonical,
            cooldown_minutes=minutes,
            reason="manual",
        ))

    def get_cooldowns(self) -> Dict[str, int]:
        return self.state.get_all_cooldowns()

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — event subscription
    # ─────────────────────────────────────────────────────────────────────────

    def subscribe(
        self,
        event_type: Type[BaseEvent],
        callback: Callable,
        async_dispatch: bool = True,
    ) -> None:
        """Subscribe to any engine event. async_dispatch=True is recommended."""
        self.events.subscribe(event_type, callback, async_dispatch=async_dispatch)

    def get_event_history(self, event_type=None, limit: int = 50) -> List:
        return self.events.get_history(event_type=event_type, limit=limit)

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — strategy info
    # ─────────────────────────────────────────────────────────────────────────

    def get_strategy_stats(self) -> Dict:
        return self.state.get_all_strategy_stats()

    def get_asset_list(self) -> List:
        """Return the canonical asset list for scanning."""
        return self.registry.all_assets()

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: engine init
    # ─────────────────────────────────────────────────────────────────────────

    def _init_engine(self) -> bool:
        """
        Lazily import and initialise UltimateTradingSystem.
        Runs on the trading loop thread to avoid import-time side effects.
        Returns True on success.
        """
        try:
            logger.info("[TradingCore] Loading UltimateTradingSystem…")
            from trading_system import UltimateTradingSystem

            self._engine = UltimateTradingSystem(
                account_balance=self.state.balance,
                strategy_mode=self.strategy_mode,
                no_telegram=self.no_telegram,
                # Inject core singletons so engine uses them
                _trading_core=self,
            )

            # Sync paper_trader state with SystemState
            self._sync_paper_trader_to_state()

            # Wire paper_trader events → EventBus
            self._wire_paper_trader_events()

            self._engine_ready.set()
            logger.info(
                f"[TradingCore] Engine ready — "
                f"strategy={self.strategy_mode} "
                f"balance=${self.state.balance:.2f}"
            )

            self.events.emit(SystemStartedEvent(
                strategy_mode=self.strategy_mode,
                account_balance=self.state.balance,
                asset_count=len(self.registry.all_assets()),
            ))
            return True

        except Exception as e:
            logger.error(f"[TradingCore] Engine init failed: {e}", exc_info=True)
            return False

    def _sync_paper_trader_to_state(self) -> None:
        """
        On startup: reconcile open positions between SystemState and
        PaperTrader.  SystemState is authoritative (loaded from disk).
        PaperTrader gets the positions injected so SL/TP monitoring works.
        """
        if not self._engine or not hasattr(self._engine, "paper_trader"):
            return

        pt          = self._engine.paper_trader
        state_pos   = {p["trade_id"]: p for p in self.state.get_open_positions()}
        trader_pos  = dict(pt.open_positions)  # {trade_id: PaperTrade}

        # Positions in state but not in paper_trader → inject
        from paper_trader import PaperTrade
        injected = 0
        for tid, pos in state_pos.items():
            if tid not in trader_pos:
                try:
                    t = pt._dict_to_paper_trade(pos)
                    if t:
                        pt.open_positions[tid] = t
                        injected += 1
                except Exception as e:
                    logger.warning(f"[TradingCore] Could not inject position {tid}: {e}")

        if injected:
            logger.info(f"[TradingCore] Injected {injected} positions into PaperTrader")

        # Sync balance
        if hasattr(self._engine, "risk_manager") and self._engine.risk_manager:
            self._engine.risk_manager.account_balance = self.state.balance

    def _wire_paper_trader_events(self) -> None:
        """
        Connect PaperTrader callbacks to TradingCore event bus.
        Called once after engine init.
        """
        if not self._engine or not hasattr(self._engine, "paper_trader"):
            return

        pt = self._engine.paper_trader

        # on_trade_closed fires when PaperTrader closes a position
        def _on_closed(trade_dict: Dict):
            self._handle_trade_closed(trade_dict)

        pt.on_trade_closed = _on_closed

        # Patch execute_signal to capture opened trades
        original_execute = pt.execute_signal

        def _patched_execute(signal: Dict):
            result = original_execute(signal)
            if result:
                self._handle_trade_opened(result)
            return result

        pt.execute_signal = _patched_execute

        logger.info("[TradingCore] PaperTrader events wired")

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: trade event handlers
    # ─────────────────────────────────────────────────────────────────────────

    def _handle_trade_opened(self, trade_dict: Dict) -> None:
        """Called when PaperTrader opens a new trade."""
        # Record in SystemState
        canonical = self.registry.canonical(trade_dict.get("asset", ""))
        trade_dict["canonical_asset"] = canonical
        self.state.add_position(trade_dict)

        # Emit event
        try:
            self.events.emit(TradeOpenedEvent(
                trade_id       = trade_dict.get("trade_id", ""),
                asset          = trade_dict.get("asset", ""),
                canonical_asset= canonical,
                category       = trade_dict.get("category", "unknown"),
                direction      = trade_dict.get("signal", "BUY"),
                entry_price    = float(trade_dict.get("entry_price", 0)),
                stop_loss      = float(trade_dict.get("stop_loss", 0)),
                take_profit_levels = trade_dict.get("take_profit_levels", []),
                position_size  = float(trade_dict.get("position_size", 0)),
                confidence     = float(trade_dict.get("confidence", 0)),
                strategy_id    = trade_dict.get("strategy_id", ""),
                reason         = trade_dict.get("reason", ""),
            ))
        except Exception as e:
            logger.debug(f"[TradingCore] TradeOpened event error: {e}")

        # Forward to Telegram
        if self.telegram:
            try:
                self.telegram.alert_trade_opened(trade_dict)
            except Exception:
                pass

    def _handle_trade_closed(self, trade_dict: Dict) -> None:
        """Called when PaperTrader closes a position (via on_trade_closed)."""
        tid  = trade_dict.get("trade_id", "")
        pnl  = float(trade_dict.get("pnl", 0))
        canonical = self.registry.canonical(trade_dict.get("asset", ""))

        # Close in SystemState
        self.state.close_position(
            trade_id    = tid,
            exit_price  = float(trade_dict.get("exit_price", 0)),
            exit_reason = trade_dict.get("exit_reason", ""),
            pnl         = pnl,
        )

        # Activate cooldown on loss
        if pnl < 0:
            self.state.set_cooldown(canonical, minutes=60)
            self.events.emit(CooldownActivatedEvent(
                asset           = trade_dict.get("asset", ""),
                canonical_asset = canonical,
                cooldown_minutes= 60,
                reason          = "loss",
            ))

        # Emit closed event
        try:
            self.events.emit(TradeClosedEvent(
                trade_id        = tid,
                asset           = trade_dict.get("asset", ""),
                canonical_asset = canonical,
                category        = trade_dict.get("category", "unknown"),
                direction       = trade_dict.get("signal", "BUY"),
                entry_price     = float(trade_dict.get("entry_price", 0)),
                exit_price      = float(trade_dict.get("exit_price", 0)),
                position_size   = float(trade_dict.get("position_size", 0)),
                pnl             = pnl,
                pnl_percent     = float(trade_dict.get("pnl_percent", 0)),
                exit_reason     = trade_dict.get("exit_reason", ""),
                duration_minutes= int(trade_dict.get("duration_minutes", 0)),
                strategy_id     = trade_dict.get("strategy_id", ""),
            ))
        except Exception as e:
            logger.debug(f"[TradingCore] TradeClosed event error: {e}")

        # Forward to Telegram
        if self.telegram:
            try:
                self.telegram.alert_trade_closed(trade_dict)
            except Exception:
                pass

        # Publish positions update to Redis (if available)
        self._publish_positions_to_redis()

    def _emit_trade_closed(self, closed: Dict) -> None:
        """Convenience wrapper for manual closes."""
        self._handle_trade_closed(closed)

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: Redis broadcast
    # ─────────────────────────────────────────────────────────────────────────

    def _publish_positions_to_redis(self) -> None:
        """Publish position update to Redis so WebSocket clients refresh."""
        try:
            from redis_broker import broker
            if broker.is_connected:
                broker.publish_positions(
                    self.state.get_open_positions(),
                    self.state.balance,
                )
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: event wiring
    # ─────────────────────────────────────────────────────────────────────────

    def _wire_internal_events(self) -> None:
        """Wire internal cross-event reactions."""
        # When daily loss limit is hit, fire RiskLimitHit
        # (actual daily loss check is in the trading loop)
        pass

    # ─────────────────────────────────────────────────────────────────────────
    # Internal: main trading loop
    # ─────────────────────────────────────────────────────────────────────────

    def _run(self) -> None:
        """Main trading loop — runs on daemon thread."""
        # Phase 1: initialise engine (may take 10-30s for model loading)
        if not self._init_engine():
            logger.error("[TradingCore] Engine failed to initialise — trading loop exiting")
            self._is_running = False
            return

        logger.info("[TradingCore] Entering trading loop")

        while not self._stop_event.is_set():
            cycle_start = datetime.now()
            try:
                self._trading_cycle()
            except Exception as e:
                logger.error(f"[TradingCore] Cycle error: {e}", exc_info=True)

            # Emit position update every cycle (dashboard polls this)
            try:
                self.events.emit(PositionUpdateEvent(
                    open_positions=self.state.get_open_positions(),
                    balance       =self.state.balance,
                    daily_pnl     =self.state.daily_pnl,
                    daily_trades  =self.state.daily_trades,
                ))
            except Exception:
                pass

            # Publish to Redis
            self._publish_positions_to_redis()

            # Wait remainder of 60s cycle
            elapsed = (datetime.now() - cycle_start).total_seconds()
            wait    = max(5.0, 60.0 - elapsed)
            self._stop_event.wait(timeout=wait)

        logger.info("[TradingCore] Trading loop exited")

    def _trading_cycle(self) -> None:
        """
        One complete scan cycle.
        This delegates to UltimateTradingSystem's scan loop but adds
        SystemState awareness for duplicate checks and cooldown enforcement.
        """
        if not self._engine:
            return

        # Day rollover check
        if self.state.check_day_rollover():
            # Reset daily loss limit in engine too
            if hasattr(self._engine, "daily_loss_limit") and self._engine.daily_loss_limit:
                self._engine.daily_loss_limit.reset_daily()
                self._engine.daily_loss_limit.set_initial_balance(self.state.balance)

        # Run parallel signal scan
        try:
            signals = self._engine.scan_all_assets_parallel()
        except Exception as e:
            logger.error(f"[TradingCore] Scan error: {e}")
            return

        if not signals:
            return

        logger.info(f"[TradingCore] Scan complete — {len(signals)} signal(s) found")

        # ── Process top signals ────────────────────────────────────────────
        processed = 0
        for signal in signals[:3]:
            if self._stop_event.is_set():
                break
            try:
                executed = self._process_signal(signal)
                if executed:
                    processed += 1
            except Exception as e:
                logger.error(
                    f"[TradingCore] Signal processing error for "
                    f"{signal.get('asset','?')}: {e}"
                )

        if processed:
            logger.info(f"[TradingCore] Executed {processed} trade(s) this cycle")

    def _process_signal(self, signal: Dict) -> bool:
        """
        Run all portfolio checks against SystemState, then execute.
        Returns True if trade was executed.
        """
        asset     = signal.get("asset", "")
        canonical = self.registry.canonical(asset)
        category  = self.registry.category(canonical)

        signal["canonical_asset"] = canonical
        signal["category"]        = category

        # ── Gate 1: Cooldown ──────────────────────────────────────────────
        if self.state.is_cooling_down(canonical):
            remaining = self.state.cooldown_remaining(canonical)
            self.events.emit(SignalRejectedEvent(
                asset=asset, canonical_asset=canonical, category=category,
                direction=signal.get("signal", "?"),
                confidence=signal.get("confidence", 0),
                reject_reason=f"Cooldown active ({remaining}min remaining)",
                reject_layer="cooldown",
            ))
            return False

        # ── Gate 2: Duplicate position ────────────────────────────────────
        if self.state.has_open_position_for(canonical):
            self.events.emit(SignalRejectedEvent(
                asset=asset, canonical_asset=canonical, category=category,
                direction=signal.get("signal", "?"),
                confidence=signal.get("confidence", 0),
                reject_reason=f"Already have open position for {canonical}",
                reject_layer="duplicate_guard",
            ))
            return False

        # ── Gate 3: Max positions ──────────────────────────────────────────
        open_count = self.state.open_position_count()
        if open_count >= 5:
            self.events.emit(SignalRejectedEvent(
                asset=asset, canonical_asset=canonical, category=category,
                direction=signal.get("signal", "?"),
                confidence=signal.get("confidence", 0),
                reject_reason="Max positions (5) reached",
                reject_layer="risk_check",
            ))
            self.events.emit(RiskLimitHitEvent(
                limit_type="max_positions",
                value=open_count, threshold=5,
                message="Maximum concurrent positions reached",
            ))
            return False

        # ── Gate 4: Category cap ──────────────────────────────────────────
        cat_positions = [
            p for p in self.state.get_open_positions()
            if p.get("category") == category
        ]
        cat_cap = self.registry.category_cap(category)
        if len(cat_positions) >= cat_cap:
            self.events.emit(SignalRejectedEvent(
                asset=asset, canonical_asset=canonical, category=category,
                direction=signal.get("signal", "?"),
                confidence=signal.get("confidence", 0),
                reject_reason=f"Category cap {category} ({len(cat_positions)}/{cat_cap})",
                reject_layer="risk_check",
            ))
            return False

        # ── Gate 5: Daily loss limit ──────────────────────────────────────
        if hasattr(self._engine, "daily_loss_limit") and self._engine.daily_loss_limit:
            trading_allowed, msg = self._engine.daily_loss_limit.update(
                self.state.daily_pnl
            )
            if not trading_allowed:
                self.events.emit(RiskLimitHitEvent(
                    limit_type="daily_loss",
                    value=self.state.daily_pnl, threshold=0,
                    message=msg,
                ))
                return False

        # ── Execute ───────────────────────────────────────────────────────
        try:
            trade = self._engine.paper_trader.execute_signal(signal)
            if trade:
                # _handle_trade_opened is wired via patched execute_signal
                self.events.emit(SignalGeneratedEvent(
                    asset        = asset,
                    canonical_asset = canonical,
                    category     = category,
                    direction    = signal.get("signal", "?"),
                    confidence   = signal.get("confidence", 0),
                    entry_price  = signal.get("entry_price", 0),
                    stop_loss    = signal.get("stop_loss", 0),
                    take_profit  = signal.get("take_profit", 0),
                    strategy_id  = signal.get("strategy_id", ""),
                    layer_reached= 7,
                    reason       = signal.get("reason", ""),
                ))
                return True
            return False

        except Exception as e:
            logger.error(f"[TradingCore] Execute signal failed for {asset}: {e}")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Diagnostics
    # ─────────────────────────────────────────────────────────────────────────

    def health_report(self) -> Dict:
        """Return a health snapshot for monitoring/dashboard."""
        import psutil
        try:
            ram_pct = psutil.virtual_memory().percent
            cpu_pct = psutil.cpu_percent(interval=0)
        except Exception:
            ram_pct = cpu_pct = 0.0

        issues = []
        if not self._engine_ready.is_set():
            issues.append("Engine not ready")

        return {
            "is_running":    self._is_running,
            "engine_ready":  self._engine_ready.is_set(),
            "strategy_mode": self.strategy_mode,
            "balance":       self.state.balance,
            "open_positions":self.state.open_position_count(),
            "daily_trades":  self.state.daily_trades,
            "daily_pnl":     self.state.daily_pnl,
            "active_cooldowns": len(self.state.get_all_cooldowns()),
            "ram_pct":       ram_pct,
            "cpu_pct":       cpu_pct,
            "issues":        issues,
            "status":        "healthy" if not issues else "degraded",
        }

    def __repr__(self) -> str:
        return (
            f"TradingCore(strategy={self.strategy_mode}, "
            f"balance={self.state.balance:.2f}, "
            f"running={self._is_running}, "
            f"positions={self.state.open_position_count()})"
        )