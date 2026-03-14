"""
core/engine.py — TradingCore: single central engine.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from utils.logger import get_logger
from core.signal import Signal
from core.pipeline import Pipeline, pipeline as _global_pipeline

logger = get_logger()


class TradingCore:
    """Central trading engine — single instance per process."""

    def __init__(
        self,
        balance: float = 30.0,
        strategy_mode: str = "voting",
        no_telegram: bool = False,
    ) -> None:
        self.balance       = balance
        self.strategy_mode = strategy_mode
        self.no_telegram   = no_telegram

        from core.state  import SystemState
        from core.events import EventBus
        from core.assets import AssetRegistry

        self.state    = SystemState()
        self.events   = EventBus()
        self.registry = AssetRegistry()
        self.pipeline: Pipeline = _global_pipeline

        if self.state.open_position_count() == 0:
            self.state.set_balance(balance, "startup")
        else:
            logger.info(
                f"[TradingCore] Restored balance=${self.state.balance:.2f} "
                f"positions={self.state.open_position_count()}"
            )

        self.telegram: Optional[Any] = None
        self.fetcher:  Optional[Any] = None

        self._engine_ready = threading.Event()
        self._stop_event   = threading.Event()
        self._is_running   = False
        self._loop_thread: Optional[threading.Thread] = None
        self._paper_trader: Optional[Any] = None
        self._risk_manager: Optional[Any] = None

        logger.info(f"[TradingCore] Init — balance=${balance} strategy={strategy_mode}")

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

    # ── Public API ────────────────────────────────────────────────────────────

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

    def get_signal_for_asset(self, asset: str) -> Optional[Dict]:
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

    def set_cooldown(self, asset: str, minutes: int = 60) -> None:
        canonical = self.registry.canonical(asset)
        self.state.set_cooldown(canonical, minutes)

    def get_cooldowns(self) -> Dict[str, int]:
        return self.state.get_all_cooldowns()

    def subscribe(self, event_type: Type, callback: Callable, async_dispatch: bool = True) -> None:
        self.events.subscribe(event_type, callback, async_dispatch=async_dispatch)

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
        try:
            from data.fetcher           import DataFetcher
            from risk.manager           import RiskManager
            from execution.paper_trader import PaperTrader
            from ml.registry            import ModelRegistry

            self.fetcher       = DataFetcher()
            self._risk_manager = RiskManager(account_balance=self.state.balance)
            self._paper_trader = PaperTrader(
                account_balance=self.state.balance,
                risk_manager=self._risk_manager,
            )

            for pos in self.state.get_open_positions():
                self._paper_trader.restore_position(pos)

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
        # Day rollover — reset risk guard to today's opening balance (Issue 6)
        rolled = self.state.check_day_rollover()
        if rolled and self._risk_manager:
            self._risk_manager.reset_daily(self.state.balance)
            logger.info(
                f"[TradingCore] New trading day — risk guard reset "
                f"at ${self.state.balance:.2f}"
            )

        if self._paper_trader:
            try:
                prices = self._get_prices()
                self._paper_trader.update_positions(prices)
                # Publish live prices to Redis (Issue 8)
                try:
                    from redis_broker import broker as _redis_broker
                    for asset, price in prices.items():
                        cat = next(
                            (p.get("category", "forex")
                             for p in self.state.get_open_positions()
                             if p.get("asset") == asset),
                            "forex",
                        )
                        _redis_broker.publish_price(asset, price, cat)
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"[TradingCore] Position update error: {e}")

        # Generate signals with per-asset contexts (Issues 2 & 9)
        signal_ctx_pairs = self._generate_signals()
        if not signal_ctx_pairs:
            return

        # Run each signal through the pipeline with its own context
        survivors = []
        for sig, ctx in signal_ctx_pairs:
            result = self.pipeline.run(sig, ctx)
            if result is not None:
                survivors.append(result)

        logger.info(
            f"[TradingCore] {len(signal_ctx_pairs)} signals → "
            f"{len(survivors)} survived pipeline"
        )

        processed = 0
        for sig in survivors[:3]:
            if self._stop_event.is_set():
                break
            if self._execute_signal(sig):
                processed += 1

        if processed:
            logger.info(f"[TradingCore] Executed {processed} trade(s)")

    def _generate_signals(self) -> List[Tuple[Signal, Dict]]:
        """
        Generate signals for all assets.
        Returns List[Tuple[Signal, Dict]] — each signal paired with its own
        context dict containing price_data, spread, and ml_prediction so that
        pipeline layers receive per-asset data (Issues 2 & 9).
        """
        result: List[Tuple[Signal, Dict]] = []
        try:
            from strategies.voting import VotingStrategy
            from ml.predictor import MLPredictor

            asset_list: List[Tuple[str, str]] = self.registry.all_assets()
            strategy  = VotingStrategy()
            predictor = MLPredictor()

            for canonical, category in asset_list:
                if self._stop_event.is_set():
                    break
                try:
                    if self.state.is_cooling_down(canonical):
                        continue
                    if self.state.has_open_position_for(canonical):
                        continue

                    price_data = self._fetch_price_data(canonical, category)
                    if price_data is None or price_data.empty:
                        continue

                    # Real-time price + spread for Layer 2 / Layer 7
                    price, spread = (0.0, 0.0)
                    if self.fetcher:
                        try:
                            price, spread = self.fetcher.get_real_time_price(
                                canonical, category
                            )
                            price  = price  or 0.0
                            spread = spread or 0.0
                        except Exception:
                            pass

                    # ML prediction for Layer 1 boost/reduce
                    ml_prob, ml_conf = predictor.predict(
                        canonical, category, price_data
                    )

                    sig = strategy.generate(canonical, canonical, category, price_data)
                    if sig and sig.confidence >= 0.5:
                        ctx = self._build_context(canonical, category)
                        ctx["price_data"]    = price_data
                        ctx["spread"]        = spread
                        ctx["ml_prediction"] = ml_prob
                        ctx["ml_confidence"] = ml_conf
                        result.append((sig, ctx))

                except Exception as e:
                    logger.debug(f"[TradingCore] Signal gen {canonical}: {e}")

        except Exception as e:
            logger.error(f"[TradingCore] Signal generation error: {e}")

        return result

    def _execute_signal(self, signal: Signal) -> bool:
        if self.state.open_position_count() >= 5:
            return False

        from config.config import CATEGORY_CAPS
        cat = signal.category
        cat_open = sum(
            1 for p in self.state.get_open_positions()
            if p.get("category") == cat
        )
        if cat_open >= CATEGORY_CAPS.get(cat, 99):
            return False

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
                # Publish signal + positions to Redis for dashboard (Issue 8)
                try:
                    from redis_broker import broker as _redis_broker
                    _redis_broker.publish_signal(signal.to_dict())
                    _redis_broker.publish_positions(
                        self.state.get_open_positions(),
                        self.state.balance,
                    )
                except Exception:
                    pass

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
            "asset":      asset,
            "category":   category,
            "balance":    self.state.balance,
            "open_count": self.state.open_position_count(),
            "daily_pnl":  self.state.daily_pnl,
            "engine":     self,
            "fetcher":    self.fetcher,
        }

    def _notify_telegram_open(self, trade: Dict) -> None:
        if not self.telegram:
            return
        try:
            # Support TelegramCommander (has method directly) and
            # TelegramManager (wraps commander in .bot) — Issue 4
            target = getattr(self.telegram, "bot", self.telegram)
            if hasattr(target, "alert_trade_opened"):
                target.alert_trade_opened(trade)
        except Exception as e:
            logger.debug(f"[TradingCore] Telegram alert failed: {e}")

    def get_asset_list(self) -> List[Tuple[str, str]]:
        return self.registry.all_assets()

    def get_strategy_stats(self) -> Dict:
        return self.state.get_all_strategy_stats()

    def close_position_manually(self, trade_id: str) -> Optional[Dict]:
        pos = self.state.get_open_position(trade_id)
        if not pos:
            return None
        entry     = float(pos.get("entry_price", 0))
        direction = pos.get("direction", pos.get("signal", "BUY"))
        size      = float(pos.get("position_size", 0))
        pnl       = 0.0
        if self.fetcher:
            try:
                price, _ = self.fetcher.get_real_time_price(
                    pos.get("asset", ""), pos.get("category", "forex")
                )
                if price:
                    pnl = (price - entry) * size if direction == "BUY" else (entry - price) * size
                    return self.state.close_position(trade_id, price, "Manual Close", pnl)
            except Exception:
                pass
        return self.state.close_position(trade_id, entry, "Manual Close", 0.0)

    def __repr__(self) -> str:
        return (
            f"TradingCore(strategy={self.strategy_mode}, "
            f"balance={self.state.balance:.2f}, "
            f"running={self._is_running}, "
            f"positions={self.state.open_position_count()})"
        )