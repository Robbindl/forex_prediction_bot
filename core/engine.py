"""
core/engine.py — TradingCore: single central engine. Clean rewrite.

All subsystems (dashboard, Telegram, auto-trainer) talk to this one object.
The trading loop runs on a daemon thread. State is persisted via core/state.py.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type

from utils.logger import get_logger
from core.signal import Signal
from core.pipeline import Pipeline, pipeline as _global_pipeline

logger = get_logger()


class TradingCore:
    """
    Central trading engine — single instance per process.
    """

    def __init__(
        self,
        balance: float = 30.0,
        strategy_mode: str = "voting",
        no_telegram: bool = False,
    ) -> None:
        self.balance       = balance
        self.strategy_mode = strategy_mode
        self.no_telegram   = no_telegram

        # ── Deferred imports (avoid circular at module level) ─────────────
        from core.state  import SystemState
        from core.events import EventBus
        from core.assets import AssetRegistry

        self.state    = SystemState()
        self.events   = EventBus()
        self.registry = AssetRegistry()
        self.pipeline: Pipeline = _global_pipeline

        # Set / restore balance
        if self.state.open_position_count() == 0:
            self.state.set_balance(balance, "startup")
        else:
            logger.info(
                f"[TradingCore] Restored balance=${self.state.balance:.2f} "
                f"positions={self.state.open_position_count()}"
            )

        # ── Subsystem handles (set externally after construction) ─────────
        self.telegram: Optional[Any] = None
        self.fetcher:  Optional[Any] = None

        # ── Loop state ────────────────────────────────────────────────────
        self._engine_ready = threading.Event()
        self._stop_event   = threading.Event()
        self._is_running   = False
        self._loop_thread: Optional[threading.Thread] = None
        self._paper_trader: Optional[Any] = None
        self._risk_manager: Optional[Any] = None

        logger.info(
            f"[TradingCore] Init — balance=${balance} strategy={strategy_mode}"
        )

    # ── Startup / Shutdown ────────────────────────────────────────────────────

    def start(self) -> None:
        if self._is_running:
            logger.warning("[TradingCore] Already running")
            return
        self._is_running = True
        self._stop_event.clear()
        self._loop_thread = threading.Thread(
            target=self._run, name="TradingCore-loop", daemon=True
        )
        self._loop_thread.start()
        logger.info("[TradingCore] Trading loop started")

    def stop(self, reason: str = "manual") -> None:
        if not self._is_running:
            return
        logger.info(f"[TradingCore] Stopping — {reason}")
        self._stop_event.set()
        self._is_running = False
        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=10)
        self.state.force_save()
        logger.info("[TradingCore] Stopped")

    def wait_until_ready(self, timeout: float = 60.0) -> bool:
        return self._engine_ready.wait(timeout=timeout)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_ready(self) -> bool:
        return self._engine_ready.is_set()

    # ── Public API — positions ────────────────────────────────────────────────

    def get_positions(self) -> List[Dict]:
        return self.state.get_open_positions()

    def get_closed_trades(self, limit: int = 100) -> List[Dict]:
        return self.state.get_closed_positions(limit=limit)

    def get_balance(self) -> float:
        return self.state.balance

    def get_performance(self) -> Dict:
        return self.state.get_performance()

    def get_daily_stats(self) -> Dict:
        return {"daily_trades": self.state.daily_trades, "daily_pnl": self.state.daily_pnl}

    # ── Public API — signals ──────────────────────────────────────────────────

    def get_signal_for_asset(self, asset: str) -> Optional[Dict]:
        """On-demand pipeline run for a single asset (used by Telegram /signal)."""
        if not self.is_ready:
            return None
        try:
            canonical = self.registry.canonical(asset)
            category  = self.registry.category(canonical)
            ctx       = self._build_context(canonical, category)
            sig = Signal(
                asset=asset, canonical_asset=canonical,
                direction="BUY", category=category, confidence=0.5,
            )
            result = self.pipeline.run(sig, ctx)
            return result.to_dict() if result else None
        except Exception as e:
            logger.error(f"[TradingCore] get_signal_for_asset({asset}): {e}")
            return None

    # ── Public API — cooldowns ────────────────────────────────────────────────

    def set_cooldown(self, asset: str, minutes: int = 60) -> None:
        canonical = self.registry.canonical(asset)
        self.state.set_cooldown(canonical, minutes)

    def get_cooldowns(self) -> Dict[str, int]:
        return self.state.get_all_cooldowns()

    # ── Public API — events ───────────────────────────────────────────────────

    def subscribe(self, event_type: Type, callback: Callable, async_dispatch: bool = True) -> None:
        self.events.subscribe(event_type, callback, async_dispatch=async_dispatch)

    # ── Public API — health ───────────────────────────────────────────────────

    def health_report(self) -> Dict:
        try:
            import psutil
            ram = psutil.virtual_memory().percent
            cpu = psutil.cpu_percent(interval=0)
        except Exception:
            ram = cpu = 0.0
        issues = [] if self._engine_ready.is_set() else ["Engine initialising"]
        return {
            "is_running":       self._is_running,
            "engine_ready":     self._engine_ready.is_set(),
            "strategy_mode":    self.strategy_mode,
            "balance":          self.state.balance,
            "open_positions":   self.state.open_position_count(),
            "daily_trades":     self.state.daily_trades,
            "daily_pnl":        self.state.daily_pnl,
            "active_cooldowns": len(self.state.get_all_cooldowns()),
            "ram_pct":          ram,
            "cpu_pct":          cpu,
            "issues":           issues,
            "status":           "healthy" if not issues else "degraded",
        }

    # ── Internal — init ───────────────────────────────────────────────────────

    def _init_subsystems(self) -> bool:
        """Initialise fetcher, risk manager, paper trader, ML."""
        try:
            from data.fetcher   import DataFetcher
            from risk.manager   import RiskManager
            from execution.paper_trader import PaperTrader
            from ml.registry    import ModelRegistry

            self.fetcher       = DataFetcher()
            self._risk_manager = RiskManager(account_balance=self.state.balance)
            self._paper_trader = PaperTrader(
                account_balance=self.state.balance,
                risk_manager=self._risk_manager,
            )

            # Restore open positions into paper trader
            for pos in self.state.get_open_positions():
                self._paper_trader.restore_position(pos)

            # Load ML models (non-blocking — models load in background if missing)
            try:
                registry = ModelRegistry()
                registry.load_all()
            except Exception as me:
                logger.warning(f"[TradingCore] ML registry load warning: {me}")

            self._engine_ready.set()
            logger.info("[TradingCore] All subsystems ready")
            return True

        except Exception as e:
            logger.error(f"[TradingCore] Subsystem init failed: {e}", exc_info=True)
            return False

    # ── Internal — main loop ──────────────────────────────────────────────────

    def _run(self) -> None:
        if not self._init_subsystems():
            logger.error("[TradingCore] Init failed — loop exiting")
            self._is_running = False
            return

        logger.info("[TradingCore] Entering trading loop")
        from config.config import SCAN_INTERVAL_SECONDS

        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            try:
                self._trading_cycle()
            except Exception as e:
                logger.error(f"[TradingCore] Cycle error: {e}", exc_info=True)

            # Emit position update event
            try:
                from core.events import PositionUpdateEvent
                self.events.emit(PositionUpdateEvent(
                    open_positions=self.state.get_open_positions(),
                    balance=self.state.balance,
                    daily_pnl=self.state.daily_pnl,
                    daily_trades=self.state.daily_trades,
                ))
            except Exception:
                pass

            elapsed = time.monotonic() - cycle_start
            wait    = max(5.0, SCAN_INTERVAL_SECONDS - elapsed)
            self._stop_event.wait(timeout=wait)

        logger.info("[TradingCore] Loop exited")

    def _trading_cycle(self) -> None:
        # Day rollover
        self.state.check_day_rollover()

        # Enforce SL/TP on open positions
        if self._paper_trader:
            try:
                self._paper_trader.update_positions(self._get_prices())
            except Exception as e:
                logger.error(f"[TradingCore] Position update error: {e}")

        # Generate and run signals through pipeline
        signals = self._generate_signals()
        if not signals:
            return

        survivors = self.pipeline.run_batch(signals, self._build_context())
        logger.info(f"[TradingCore] {len(signals)} signals → {len(survivors)} survived pipeline")

        processed = 0
        for sig in survivors[:3]:
            if self._stop_event.is_set():
                break
            if self._execute_signal(sig):
                processed += 1

        if processed:
            logger.info(f"[TradingCore] Executed {processed} trade(s)")

    def _generate_signals(self) -> List[Signal]:
        """Ask each strategy to generate signals for all assets."""
        signals: List[Signal] = []
        try:
            from strategies.voting import VotingStrategy
            assets = self.registry.all_assets()
            strategy = VotingStrategy()
            for asset in assets:
                if self._stop_event.is_set():
                    break
                try:
                    canonical = self.registry.canonical(asset)
                    category  = self.registry.category(canonical)

                    if self.state.is_cooling_down(canonical):
                        continue
                    if self.state.has_open_position_for(canonical):
                        continue

                    price_data = self._fetch_price_data(asset, category)
                    if price_data is None or price_data.empty:
                        continue

                    sig = strategy.generate(asset, canonical, category, price_data)
                    if sig and sig.confidence >= 0.5:
                        signals.append(sig)
                except Exception as e:
                    logger.debug(f"[TradingCore] Signal gen {asset}: {e}")
        except Exception as e:
            logger.error(f"[TradingCore] Signal generation error: {e}")
        return signals

    def _execute_signal(self, signal: Signal) -> bool:
        """Run portfolio gates then execute via paper trader."""
        canonical = signal.canonical_asset or signal.asset

        # Gate: max positions
        if self.state.open_position_count() >= 5:
            return False

        # Gate: category cap
        from config.config import CATEGORY_CAPS
        cat = signal.category
        cat_open = sum(
            1 for p in self.state.get_open_positions()
            if p.get("category") == cat
        )
        if cat_open >= CATEGORY_CAPS.get(cat, 99):
            return False

        # Execute
        try:
            trade = self._paper_trader.execute_signal(signal.to_dict())
            if trade:
                self.state.add_position(trade)
                logger.log_trade(
                    "OPEN",
                    asset=signal.asset,
                    direction=signal.direction,
                    confidence=f"{signal.confidence:.3f}",
                    entry=signal.entry_price,
                )
                self._notify_telegram_open(trade)
                return True
        except Exception as e:
            logger.error(f"[TradingCore] Execute failed {signal.asset}: {e}")
        return False

    def _fetch_price_data(self, asset: str, category: str):
        if self.fetcher:
            try:
                return self.fetcher.get_ohlcv(asset, category)
            except Exception:
                pass
        return None

    def _get_prices(self) -> Dict[str, float]:
        prices = {}
        for pos in self.state.get_open_positions():
            asset    = pos.get("asset", "")
            category = pos.get("category", "forex")
            if asset and self.fetcher:
                try:
                    price, _ = self.fetcher.get_real_time_price(asset, category)
                    if price:
                        prices[asset] = price
                except Exception:
                    pass
        return prices

    def _build_context(self, asset: str = "", category: str = "") -> Dict[str, Any]:
        return {
            "asset":       asset,
            "category":    category,
            "balance":     self.state.balance,
            "open_count":  self.state.open_position_count(),
            "daily_pnl":   self.state.daily_pnl,
            "engine":      self,
            "fetcher":     self.fetcher,
        }

    def _notify_telegram_open(self, trade: Dict) -> None:
        if self.telegram:
            try:
                self.telegram.alert_trade_opened(trade)
            except Exception:
                pass

    def get_asset_list(self) -> List:
        return self.registry.all_assets()

    def get_strategy_stats(self) -> Dict:
        return self.state.get_all_strategy_stats()

    def __repr__(self) -> str:
        return (
            f"TradingCore(strategy={self.strategy_mode}, "
            f"balance={self.state.balance:.2f}, "
            f"running={self._is_running}, "
            f"positions={self.state.open_position_count()})"
        )