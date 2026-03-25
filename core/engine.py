"""
core/engine.py — TradingCore: single central engine.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Type

from config.config import MIN_FINAL_CONFIDENCE
from utils.logger import get_logger
from core.signal import Signal
from core.pipeline import Pipeline, pipeline as _global_pipeline

TRADE_CLOSE_COOLDOWN_MINUTES = 60
TRADE_MIN_CONFIDENCE = MIN_FINAL_CONFIDENCE  # follow config value from .env

logger = get_logger()


def _get_news_event(category: str) -> dict:
    """Get current news event state — graceful fallback if monitor not started."""
    try:
        from data_ingestion.news_event_monitor import news_monitor
        return news_monitor.get_event_state(category)
    except Exception:
        return {"state": "clear", "event": "", "impact": "", "direction": "", "mins_to": 0}


class TradingCore:
    """Central trading engine — single instance per process."""

    def __init__(
        self,
        balance: float = 10000.0,  # FIX: Changed from $30 to realistic balance
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

        from pathlib import Path as _Path
        _state_file_exists = _Path("data/system_state.json").exists()

        if self.state.open_position_count() == 0 and not _state_file_exists:
            # First ever run — no saved state at all — use the supplied balance
            self.state.set_balance(balance, "startup")
            logger.info(f"[TradingCore] Fresh start — balance=${balance}")
        else:
            # Restart — preserve accumulated balance from previous session
            logger.info(
                f"[TradingCore] Restored balance=${self.state.balance:.2f} "
                f"positions={self.state.open_position_count()}"
            )

        self.telegram:    Optional[Any] = None
        self.fetcher:     Optional[Any] = None
        self._strategy:   Optional[Any] = None   # VotingStrategy singleton
        self._predictor:  Optional[Any] = None   # MLPredictor singleton

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

            # ── Weekend market-hours guard ─────────────────────────────────
            # The dashboard calls this for every asset on a refresh loop.
            # Without this guard it sends forex/commodities/indices through
            # the full pipeline on weekends, producing misleading MetaAI logs.
            if category != "crypto":
                from datetime import datetime as _dt, timezone as _tz
                _now  = _dt.now(tz=_tz.utc)
                _wd   = _now.weekday()
                _hour = _now.hour
                _closed = (
                    _wd == 5
                    or (_wd == 6 and _hour < 22)
                    or (_wd == 4 and _hour >= 22)
                )
                if _closed:
                    return None
            # ──────────────────────────────────────────────────────────────

            ctx = self._build_context(canonical, category)
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

            # ── Singleton strategy + predictor — created once, reused every cycle ──
            from strategies.voting import VotingStrategy
            from ml.predictor      import MLPredictor
            self._strategy  = VotingStrategy()
            self._predictor = MLPredictor()
            logger.info("[TradingCore] VotingStrategy + MLPredictor initialised (singletons)")
            self._paper_trader = PaperTrader(
                account_balance=self.state.balance,
                risk_manager=self._risk_manager,
            )

            def _on_trade_closed(trade: dict) -> None:
                try:
                    trade_id    = trade.get("trade_id", "")
                    exit_price  = float(trade.get("exit_price", 0))
                    exit_reason = trade.get("exit_reason", "Unknown")
                    pnl         = float(trade.get("pnl", 0))

                    self.state.close_position(trade_id, exit_price, exit_reason, pnl)
                    self._risk_manager.update_balance(self.state.balance)

                    # ── Telegram close alert ──────────────────────────────────────────
                    if self.telegram:
                        try:
                            target = getattr(self.telegram, "bot", self.telegram)
                            if hasattr(target, "alert_trade_closed"):
                                target.alert_trade_closed(trade)
                        except Exception:
                            pass

                    # ── Robbie learns from it ─────────────────────────────────────────
                    try:
                        from services.personality_service import personality as _personality
                        _personality.record_trade(trade)
                    except Exception:
                        pass

                    # ── Phase 11 — record trade result for win rate tracking ──────────
                    try:
                        from monitoring.system_health_service import monitor as _mon
                        _mon.record_trade_result(pnl)
                    except Exception:
                        pass

                    logger.log_trade("CLOSE", trade_id=trade_id,
                                    asset=trade.get("asset", ""),
                                    pnl=round(pnl, 4), reason=exit_reason)
                    try:
                        canonical = self.registry.canonical(trade.get("asset", ""))
                        self.state.set_cooldown(canonical, TRADE_CLOSE_COOLDOWN_MINUTES)
                        logger.info(
                            f"[TradingCore] Set cooldown {TRADE_CLOSE_COOLDOWN_MINUTES}m "
                            f"for {canonical} after close"
                        )
                    except Exception:
                        pass
                except Exception as e:
                    logger.error(f"[TradingCore] on_trade_closed error: {e}")

            self._paper_trader.on_trade_closed = _on_trade_closed
            # ──────────────────────────────────────────────────────────────────

            for pos in self.state.get_open_positions():
                self._paper_trader.restore_position(pos)

            # ── Offline gap-fill check ────────────────────────────────────────
            # For every restored position, scan OHLCV history from open_time
            # to now and close any position whose SL or TP was breached while
            # the bot was offline. First breach chronologically wins.
            self._check_offline_sl_tp()

            try:
                registry = ModelRegistry()
                registry.load_all()
            except Exception as me:
                logger.warning(f"[TradingCore] ML registry load warning: {me}")

            # Pre-warm OHLCV cache in background — avoids slow first trading cycle
            import threading as _threading
            def _prewarm():
                try:
                    asset_list = self.registry.all_assets()
                    for canonical, category in asset_list:
                        if self._stop_event.is_set():
                            break
                        try:
                            self.fetcher.get_ohlcv(canonical, category)
                        except Exception:
                            pass
                    logger.info("[TradingCore] OHLCV cache pre-warmed")
                except Exception as e:
                    logger.debug(f"[TradingCore] Pre-warm error: {e}")
            _threading.Thread(target=_prewarm, name="ohlcv-prewarm", daemon=True).start()

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
                # Publish every survivor to Redis immediately so the dashboard
                # _sig_store is updated in real time without running the pipeline
                # a second time. Previously only executed trades were published,
                # forcing the dashboard to independently re-run the pipeline.
                try:
                    from redis_broker import broker as _redis_broker
                    _redis_broker.publish_signal(result.to_dict())
                except Exception:
                    pass

        logger.info(
            f"[TradingCore] {len(signal_ctx_pairs)} signals → "
            f"{len(survivors)} survived pipeline"
        )

        processed = 0
        for sig in survivors[:3]:
            if self._stop_event.is_set():
                break
            if sig.confidence < TRADE_MIN_CONFIDENCE:
                logger.info(
                    f"[TradingCore] Skipping execution for {sig.asset} due to confidence "
                    f"{sig.confidence:.3f} < {TRADE_MIN_CONFIDENCE}"
                )
                continue
            if self._execute_signal(sig):
                processed += 1

        if processed:
            logger.info(f"[TradingCore] Executed {processed} trade(s)")

    def _generate_signals(self) -> List[Tuple[Signal, Dict]]:
        """
        Generate signals for all assets concurrently.
        """
        result: List[Tuple[Signal, Dict]] = []
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            asset_list: List[Tuple[str, str]] = self.registry.all_assets()
            # Reuse the singletons initialised in _init_subsystems —
            # no new object creation every 45 seconds
            strategy  = self._strategy
            predictor = self._predictor
            if strategy is None or predictor is None:
                logger.warning(f"[TradingCore] Strategy/predictor not ready — strategy={strategy is not None}, predictor={predictor is not None} — skipping cycle")
                return result

            candidates = [
                (canonical, category) for canonical, category in asset_list
                if not self.state.is_cooling_down(canonical)
                and not self.state.has_open_position_for(canonical)
            ]
            logger.debug(f"[TradingCore] Starting signal generation for {len(candidates)} candidates")
            logger.info(
                f"[TradingCore] Asset scan: total={len(asset_list)} candidates={len(candidates)} "
                f"cooling={len([a for a, _ in asset_list if self.state.is_cooling_down(a)])} "
                f"open_pos={len([a for a, _ in asset_list if self.state.has_open_position_for(a)])}"
            )
            
            if not candidates:
                logger.warning("[TradingCore] No candidates available for signal generation")
                return result

            if self._stop_event.is_set():
                return result

            def _process_asset(canonical_category):
                canonical, category = canonical_category
                if self._stop_event.is_set():
                    return None

                # ── Market hours pre-filter ───────────────────────────────────
                # Skip non-crypto assets when their market is closed.
                # Implemented inline so a failed import never silently lets
                # closed-market signals through to the pipeline.
                # Previously used `except Exception: pass` which caused forex,
                # commodities and indices to leak through on Saturdays.
                try:
                    from datetime import datetime as _dt, timezone as _tz
                    _now  = _dt.now(tz=_tz.utc)
                    _wd   = _now.weekday()   # 0=Mon … 5=Sat … 6=Sun
                    _hour = _now.hour
                    _weekday_name = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][_wd]

                    if category == "crypto":
                        pass  # 24/7 — always open

                    elif _wd >= 5:
                        # Saturday (5) or Sunday (6) — all non-crypto closed
                        # Exception: forex and commodities open Sunday ≥ 22:00
                        if _wd == 6 and _hour >= 22 and category in ("forex", "commodities"):
                            pass  # Sunday evening session open
                        else:
                            logger.debug(f"[TradingCore] {canonical} ({category}) BLOCKED: weekend {_weekday_name} {_hour:02d}:00 UTC")
                            return None

                    else:
                        # Weekday — apply per-category hours
                        if category == "forex":
                            # Closed Friday after 22:00 UTC
                            if _wd == 4 and _hour >= 22:
                                logger.debug(f"[TradingCore] {canonical} ({category}) BLOCKED: Fri after 22:00")
                                return None

                        elif category in ("stocks", "indices"):
                            # NYSE/Nasdaq: 13:00–21:00 UTC (09:30–16:00 ET approx)
                            if not (13 <= _hour < 21):
                                logger.debug(f"[TradingCore] {canonical} ({category}) BLOCKED: market hours {_hour:02d}:00 UTC (need 13:00-21:00)")
                                return None

                        elif category == "commodities":
                            # CME: closed daily 21:00–22:00 UTC (settlement break)
                            if _hour == 21:
                                logger.debug(f"[TradingCore] {canonical} ({category}) BLOCKED: CME settlement 21:00 UTC")
                                return None

                except Exception as _mh_err:
                    # Log the error rather than silently passing — if this fails
                    # we want to know why, not accidentally trade closed markets.
                    logger.warning(
                        f"[TradingCore] Market-hours pre-filter error for "
                        f"{canonical} ({category}): {_mh_err} — skipping asset"
                    )
                    return None

                try:
                    price_data = self._fetch_price_data(canonical, category)
                    if price_data is None or price_data.empty:
                        logger.debug(f"[TradingCore] {canonical}: no price data")
                        return None

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
                        logger.info(f"[TradingCore] SIGNAL: {canonical} {sig.direction} confidence={sig.confidence:.2%}")
                        return (sig, ctx)
                    elif sig:
                        logger.debug(f"[TradingCore] {canonical}: signal confidence too low ({sig.confidence:.2%})")
                except Exception as e:
                    logger.warning(f"[TradingCore] Signal gen {canonical}: {e}")
                return None

            with ThreadPoolExecutor(max_workers=6) as pool:
                futures = {pool.submit(_process_asset, ac): ac for ac in candidates}
                logger.debug(f"[TradingCore] Submitted {len(futures)} asset tasks to thread pool")
                for future in as_completed(futures):
                    if self._stop_event.is_set():
                        break
                    try:
                        res = future.result()
                        if res is not None:
                            result.append(res)
                            logger.debug(f"[TradingCore] Got signal from future: {res[0].asset}")
                        else:
                            logger.debug(f"[TradingCore] Future returned None for {futures.get(future, 'unknown')}")
                    except Exception as e:
                        asset_pair = futures.get(future, "unknown")
                        logger.error(f"[TradingCore] Future failed for {asset_pair}: {e}")
            logger.debug(f"[TradingCore] Signal generation complete: {len(result)} signals generated from {len(futures)} tasks")

        except Exception as e:
            logger.error(f"[TradingCore] Signal generation error: {e}")

        return result

    def _execute_signal(self, signal: Signal) -> bool:
        from config.config import MAX_POSITIONS
        if self.state.open_position_count() >= MAX_POSITIONS:
            return False

        from config.config import CATEGORY_CAPS
        cat = signal.category
        cat_open = sum(
            1 for p in self.state.get_open_positions()
            if p.get("category") == cat
        )
        if cat_open >= CATEGORY_CAPS.get(cat, 99):
            return False

        # FIX S6: Call validate_signal so the daily loss guard is actually
        # enforced.  Previously this was never called → 5% daily loss halt
        # had no effect → bot could blow the account in a single bad day.
        if self._risk_manager:
            allowed, reason = self._risk_manager.validate_signal(
                confidence=signal.confidence,
                daily_pnl=self.state.daily_pnl,
                category=signal.category,
            )
            if not allowed:
                logger.warning(f"[TradingCore] Risk gate blocked {signal.asset}: {reason}")
                return False

        # FIX S5: Call portfolio_risk.evaluate() so drawdown halt and
        # position concentration limits are enforced.  Previously this was
        # attached to the engine but never called in the execution path.
        if hasattr(self, "_portfolio_risk") and self._portfolio_risk is not None:
            try:
                pr_allowed, pr_reason = self._portfolio_risk.evaluate(
                    signal=signal.to_dict(),
                    open_positions=self.state.get_open_positions(),
                    balance=self.state.balance,
                )
                if not pr_allowed:
                    logger.warning(
                        f"[TradingCore] PortfolioRisk blocked {signal.asset}: {pr_reason}"
                    )
                    return False
            except Exception as _pre:
                logger.debug(f"[TradingCore] PortfolioRisk check error: {_pre}")

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
                from config.config import TRADING_TIMEFRAME
                _auto_periods = {"15m": 500, "1h": 300, "4h": 200, "1d": 100}
                _periods = _auto_periods.get(TRADING_TIMEFRAME, 100)
                return self.fetcher.get_ohlcv(
                    asset, category,
                    interval=TRADING_TIMEFRAME,
                    periods=_periods,
                )
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

    # ── Context helpers — wired so classifier receives live macro/narrative data ──

    @staticmethod
    def _get_macro_impact_static() -> str:
        """
        FIX: Read macro impact level from MacroDataCollector so the
        MarketConditionClassifier can detect crisis regimes.
        Previously this was never populated → always "LOW" → crisis unreachable.
        """
        try:
            from data_ingestion import macro_data_collector as _mdc
            collector = getattr(_mdc, "collector", None)
            if collector is not None:
                return getattr(collector, "current_impact", "LOW")
        except Exception:
            pass
        return "LOW"

    @staticmethod
    def _get_narrative_strength_static(asset: str) -> float:
        """
        FIX: Read narrative strength from Phase 4 TopicClusterEngine.
        Previously this was never populated → always 0.0 → narrative boost
        in Layer 5 never fired and crisis regime via narrative unreachable.
        """
        try:
            from narrative_ai import get_narrative_scores
            scores = get_narrative_scores()
            if scores:
                return round(max(scores.values()), 3)
        except Exception:
            pass
        return 0.0

    def _build_context(self, asset: str = "", category: str = "") -> Dict[str, Any]:
        sentiment_score = 0.0
        try:
            from layers.layer5_sentiment import _fetch_sentiment
            sentiment_score = _fetch_sentiment(asset, category)
        except Exception:
            pass

        # ── Phase 1: funding rates + OI for Meta AI Layer 8 ──────────────
        funding_bias = "NEUTRAL"
        oi_signal    = "NEUTRAL"
        try:
            from data_ingestion import funding_monitor, oi_monitor
            # Normalise asset to exchange symbol format
            symbol = (asset.replace("-USD", "USDT")
                           .replace("/", "")
                           .replace("-", ""))
            funding_bias = funding_monitor.get_bias(symbol)
            oi_signal    = oi_monitor.get_signal(symbol)
        except Exception:
            pass

        return {
            "asset":              asset,
            "category":          category,
            "balance":           self.state.balance,
            "open_count":        self.state.open_position_count(),
            "daily_pnl":         self.state.daily_pnl,
            "engine":            self,
            "fetcher":           self.fetcher,
            "sentiment_score":   sentiment_score,
            "funding_bias":      funding_bias,    # Phase 1 → Layer 8 Meta AI
            "oi_signal":         oi_signal,       # Phase 1 → Layer 8 Meta AI
            "news_event":        _get_news_event(category),  # news event state
            # FIX: wire macro_impact from MacroDataCollector so crisis regime
            # can trigger in MarketConditionClassifier.classify().
            # Previously this key was never set → macro_impact always "LOW" →
            # crisis regime unreachable via macro path.
            "macro_impact":      self._get_macro_impact_static(),
            # FIX: wire narrative_strength from Phase 4 TopicClusterEngine so
            # the crisis regime check (macro_impact=HIGH AND narrative_str>0.3)
            # has a chance of firing.  Previously always 0.0.
            "narrative_strength": self._get_narrative_strength_static(asset),
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

    def _notify_telegram_close(self, trade: Dict) -> None:
        if not self.telegram:
            return
        try:
            target = getattr(self.telegram, "bot", self.telegram)
            if hasattr(target, "alert_trade_closed"):
                target.alert_trade_closed(trade)
        except Exception as e:
            logger.debug(f"[TradingCore] Telegram close alert failed: {e}")

    def get_asset_list(self) -> List[Tuple[str, str]]:
        return self.registry.all_assets()

    def get_strategy_stats(self) -> Dict:
        return self.state.get_all_strategy_stats()

    def _check_offline_sl_tp(self) -> None:
        """
        Runs once on startup after positions are restored.
        For each open position, fetches 5m OHLCV bars from open_time to now
        and checks if SL or TP was breached while the bot was offline.
        If breached, closes the position at the breach price so P&L and
        trade history are accurate.
        """
        import yfinance as yf
        from datetime import datetime, timezone
        from data.fetcher import _yf_symbol

        positions = self.state.get_open_positions()
        if not positions:
            return

        logger.info(f"[TradingCore] Offline gap-fill: checking {len(positions)} position(s)")

        for pos in positions:
            trade_id    = pos.get("trade_id", "")
            asset       = pos.get("asset", "")
            category    = pos.get("category", "forex")
            direction   = pos.get("direction", pos.get("signal", "BUY"))
            entry       = float(pos.get("entry_price", 0))
            stop_loss   = float(pos.get("stop_loss", 0))
            take_profit = float(pos.get("take_profit", 0))
            open_time   = pos.get("open_time", "")
            size        = float(pos.get("position_size", 0))

            if not entry or not stop_loss or not asset:
                continue

            try:
                # Parse open_time to a datetime
                try:
                    dt_open = datetime.fromisoformat(open_time)
                    if dt_open.tzinfo is None:
                        dt_open = dt_open.replace(tzinfo=timezone.utc)
                except Exception:
                    logger.debug(f"[GapFill] Cannot parse open_time for {asset} — skipping")
                    continue

                dt_now = datetime.now(tz=timezone.utc)
                minutes_offline = (dt_now - dt_open).total_seconds() / 60

                # No gap to check — bot just started
                if minutes_offline < 5:
                    continue

                # Fetch 5m bars covering the offline period
                # yfinance supports 5m for up to 60 days
                sym = _yf_symbol(asset, category)
                ticker = yf.Ticker(sym)
                df = ticker.history(period="7d", interval="5m", auto_adjust=True)

                if df is None or df.empty:
                    logger.debug(f"[GapFill] No 5m data for {asset} — skipping")
                    continue

                # Filter to bars after open_time
                df.index = df.index.tz_convert("UTC") if df.index.tzinfo else df.index.tz_localize("UTC")
                df = df[df.index > dt_open].copy()

                if df.empty:
                    continue

                # Scan each bar chronologically — first breach wins
                breach_price  = None
                breach_reason = None
                breach_time   = None

                for bar_time, bar in df.iterrows():
                    bar_low  = float(bar["Low"])
                    bar_high = float(bar["High"])

                    if direction == "BUY":
                        if bar_low <= stop_loss:
                            breach_price  = stop_loss
                            breach_reason = "Stop Loss (offline)"
                            breach_time   = bar_time
                            break
                        if take_profit and bar_high >= take_profit:
                            breach_price  = take_profit
                            breach_reason = "Take Profit (offline)"
                            breach_time   = bar_time
                            break
                    else:  # SELL
                        if bar_high >= stop_loss:
                            breach_price  = stop_loss
                            breach_reason = "Stop Loss (offline)"
                            breach_time   = bar_time
                            break
                        if take_profit and bar_low <= take_profit:
                            breach_price  = take_profit
                            breach_reason = "Take Profit (offline)"
                            breach_time   = bar_time
                            break

                if breach_price is None:
                    logger.debug(f"[GapFill] {asset}: no breach found — position remains open")
                    continue

                # Calculate P&L at breach price
                pnl = (breach_price - entry) * size if direction == "BUY" else (entry - breach_price) * size

                # Close in state + DB
                closed = self.state.close_position(trade_id, breach_price, breach_reason, pnl)
                if not closed:
                    continue

                # Remove from PaperTrader
                if self._paper_trader:
                    with self._paper_trader._lock:
                        self._paper_trader.open_positions.pop(trade_id, None)

                # Set cooldown
                try:
                    canonical = self.registry.canonical(asset)
                    self.state.set_cooldown(canonical, TRADE_CLOSE_COOLDOWN_MINUTES)
                except Exception:
                    pass

                # Telegram alert
                self._notify_telegram_close(closed)

                # Side effects
                try:
                    from services.personality_service import personality as _personality
                    _personality.record_trade(closed)
                except Exception:
                    pass
                try:
                    from monitoring.system_health_service import monitor as _mon
                    _mon.record_trade_result(pnl)
                except Exception:
                    pass

                logger.info(
                    f"[GapFill] {asset} {direction} closed offline — "
                    f"{breach_reason} @ {breach_price:.5f}  "
                    f"PnL=${pnl:.2f}  breached at {breach_time}"
                )

            except Exception as e:
                logger.error(f"[GapFill] {asset} gap-fill error: {e}")

    def close_position_manually(self, trade_id: str) -> Optional[Dict]:
        pos = self.state.get_open_position(trade_id)
        if not pos:
            return None
        entry     = float(pos.get("entry_price", 0))
        direction = pos.get("direction", pos.get("signal", "BUY"))
        size      = float(pos.get("position_size", 0))
        pnl       = 0.0
        exit_price = entry

        if self.fetcher:
            try:
                price, _ = self.fetcher.get_real_time_price(
                    pos.get("asset", ""), pos.get("category", "forex")
                )
                if price:
                    exit_price = price
                    pnl = (price - entry) * size if direction == "BUY" else (entry - price) * size
            except Exception:
                pass

        # 1. Close in SystemState + DB
        closed = self.state.close_position(trade_id, exit_price, "Manual Close", pnl)
        if not closed:
            return None

        # 2. Remove from PaperTrader so it stops monitoring this ghost position.
        #    Without this the position stays in PaperTrader.open_positions forever,
        #    and when SL/TP eventually triggers it fires on_trade_closed a second time.
        if self._paper_trader:
            with self._paper_trader._lock:
                self._paper_trader.open_positions.pop(trade_id, None)

        # 3. Set cooldown so the asset is not immediately re-scanned and re-opened.
        #    Without this the next 45-second scan cycle treats the asset as a fresh
        #    candidate and re-opens the position the user just closed.
        try:
            canonical = self.registry.canonical(closed.get("asset", ""))
            self.state.set_cooldown(canonical, TRADE_CLOSE_COOLDOWN_MINUTES)
            logger.info(
                f"[TradingCore] Manual close — set cooldown {TRADE_CLOSE_COOLDOWN_MINUTES}m "
                f"for {canonical}"
            )
        except Exception as e:
            logger.debug(f"[TradingCore] Manual close cooldown error: {e}")

        # 4. Telegram close alert
        self._notify_telegram_close(closed)

        # 5. Personality + monitoring — same side effects as automatic close
        try:
            from services.personality_service import personality as _personality
            _personality.record_trade(closed)
        except Exception:
            pass
        try:
            from monitoring.system_health_service import monitor as _mon
            _mon.record_trade_result(pnl)
        except Exception:
            pass

        logger.log_trade(
            "CLOSE", trade_id=trade_id,
            asset=closed.get("asset", ""),
            pnl=round(pnl, 4), reason="Manual Close",
        )
        return closed

    def __repr__(self) -> str:
        return (
            f"TradingCore(strategy={self.strategy_mode}, "
            f"balance={self.state.balance:.2f}, "
            f"running={self._is_running}, "
            f"positions={self.state.open_position_count()})"
        )


# ── Module-level singleton reference ──────────────────────────────────────────
# Set by bot.py after engine.start() so pipeline_reporter and other modules
# can access the live engine instance without circular imports.
_CORE_INSTANCE: Optional["TradingCore"] = None