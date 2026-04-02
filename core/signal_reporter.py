from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from config.config import MIN_FINAL_CONFIDENCE
from utils.logger import get_logger
from core.signal_journal import PASS, KILLED, INFO

if TYPE_CHECKING:
    from core.signal import Signal

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
MIN_TRADES_FOR_ADJUSTMENT = 10     # need at least 10 historical trades
STRONG_EDGE_THRESHOLD     = 0.65   # win rate >= 65% = boost
POSITIVE_EDGE_THRESHOLD   = 0.55   # win rate >= 55% = small boost
WEAK_EDGE_THRESHOLD       = 0.50   # win rate < 50%  = reduce
POOR_EDGE_THRESHOLD       = 0.40   # win rate < 40%  = bigger reduce + warn
DAILY_OPTIMISE_HOUR       = 3      # run daily optimisation at 3 AM UTC
TELEGRAM_ASSET_ALERT_COOLDOWN_SECS = 3600  # 1h dedupe window per asset
TELEGRAM_SIGNAL_MIN_CONFIDENCE = MIN_FINAL_CONFIDENCE  # follow config value from .env
# Only alert on signals that will actually be executed as trades.
# Signals between 0.55-0.62 can survive the decision engine
# but are skipped by the trading loop, so no need to alert on them.

# ── Backtest cache (avoids re-running for same asset repeatedly) ──────────────
_backtest_cache:    Dict[str, dict] = {}   # asset → {result, ts}
_cache_ttl_secs    = 3600                  # cache results for 1 hour
_cache_lock        = threading.Lock()


class SignalReporter:
    """
    Singleton post-decision reporter.
    Wire into the decision engine after every evaluation.
    """

    _instance: Optional["SignalReporter"] = None
    _lock      = threading.Lock()

    def __new__(cls) -> "SignalReporter":
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._initialised = False
                cls._instance = inst
        return cls._instance

    def __init__(self) -> None:
        if self._initialised:
            return
        self._initialised    = True
        self._telegram        = None   # set by wire_telegram()
        self._pub             = None   # Redis publisher
        self._db_ok           = False
        self._daily_thread:   Optional[threading.Thread] = None
        self._last_telegram_sent: Dict[str, float] = {}
        self._init_redis()
        self._init_db()
        # FIX Race1: DailyOptimiser is now started from wire_telegram() so it
        # only runs after the engine + Telegram are fully wired.
        logger.info("[SignalReporter] Initialised")

    # ── Public API ────────────────────────────────────────────────────────────

    def wire_telegram(self, telegram_bot) -> None:
        """Call from bot.py after Telegram is started."""
        self._telegram = telegram_bot
        logger.info("[SignalReporter] Telegram wired")
        # FIX Race1: start DailyOptimiser here, after engine + Telegram ready
        self._start_daily_optimiser()

    def report(self, signal: "Signal", context: Dict[str, Any]) -> "Signal":
        """
        Called by the decision engine after every evaluation.
        Works for both surviving AND killed signals.
        Returns the signal after reporting side-effects.
        """
        try:
            # 1. Attach research/live-validation context
            signal = self._run_backtest(signal)

            # 2. Store performance in DB
            self._store_performance(signal)

            # 3. Send Telegram journal report
            self._send_telegram(signal)

            # 4. Publish to Redis for dashboard
            self._publish_redis(signal)

        except Exception as e:
            logger.error(f"[SignalReporter] report() error: {e}", exc_info=True)

        return signal

    # ── Auto-backtest (Option A) ───────────────────────────────────────────────

    def _run_backtest(self, signal: "Signal") -> "Signal":
        """
        Record the current model-research and live-validation summary.
        This replaces the old placeholder "backtest disabled" entry.
        """
        validation = signal.metadata.get("governance_validation") or {}
        research = validation.get("model_research") or {}
        live = validation.get("live_validation") or {}
        signal.journal.record(
            layer=0, name="research_validation", decision=INFO,
            reason=(
                f"wf={float(research.get('walk_forward_accuracy', 0.0)):.3f} "
                f"holdout={float(research.get('holdout_accuracy', 0.0)):.3f} "
                f"live={float(live.get('accuracy_pct', 0.0)):.1f}%"
            ),
            conf_before=signal.confidence,
            conf_after=signal.confidence,
            data={
                "model_key": validation.get("model_key"),
                "research_grade": research.get("research_grade"),
                "research_approved": research.get("research_approved"),
                "walk_forward_accuracy": research.get("walk_forward_accuracy", 0.0),
                "holdout_accuracy": research.get("holdout_accuracy", 0.0),
                "live_validation_scope": live.get("scope", "n/a"),
                "live_validation_total": live.get("total", 0),
                "live_validation_accuracy_pct": live.get("accuracy_pct", 0.0),
            },
        )
        return signal

    # ── Performance storage (Option C) ───────────────────────────────────────

    def _store_performance(self, signal: "Signal") -> None:
        """Store validation/performance data for the strategy dashboard."""
        if not self._db_ok:
            return
        try:
            from services.db_pool import get_db
            performance = self._extract_strategy_performance(signal)
            if not performance:
                return
            get_db().save_strategy_performance_snapshot(
                asset=signal.canonical_asset or signal.asset,
                category=signal.category,
                strategy_id=signal.strategy_id or "voting",
                win_rate=performance["win_rate"],
                sharpe_ratio=performance["sharpe_ratio"],
                total_trades=performance["total_trades"],
            )
        except Exception as e:
            logger.debug(f"[SignalReporter] store_performance: {e}")

    # ── Telegram (Option A) ───────────────────────────────────────────────────

    def _send_telegram(self, signal: "Signal") -> None:
        if not self._telegram:
            return
        if not signal.alive:
            logger.debug(f"[SignalReporter] Skipping Telegram for dead signal {signal.asset} {signal.direction}")
            return

        if signal.confidence < TELEGRAM_SIGNAL_MIN_CONFIDENCE:
            logger.debug(
                f"[SignalReporter] Skipping Telegram for {signal.asset} due to low final score "
                f"({signal.confidence:.3f} < {TELEGRAM_SIGNAL_MIN_CONFIDENCE})"
            )
            return

        asset_key = signal.canonical_asset or signal.asset
        now = time.time()
        last = self._last_telegram_sent.get(asset_key, 0.0)
        if now - last < TELEGRAM_ASSET_ALERT_COOLDOWN_SECS:
            logger.debug(
                f"[SignalReporter] Skipping Telegram for {asset_key} due to dedupe cooldown "
                f"({now-last:.0f}s < {TELEGRAM_ASSET_ALERT_COOLDOWN_SECS}s)"
            )
            return

        try:
            msg = signal.journal.to_telegram_plain(signal)
            logger.info(f"[SignalReporter] Sending Telegram alert: {signal.journal.final_decision()} {signal.asset} {signal.direction}")
            sent = self._telegram.send_message(msg, parse_mode=None)
            if sent:
                logger.info(f"[SignalReporter] Telegram sent for {signal.asset} {signal.direction}")
                self._last_telegram_sent[asset_key] = now
            else:
                logger.warning(f"[SignalReporter] Telegram send failed for {signal.asset} {signal.direction}")
        except Exception as e:
            logger.debug(f"[SignalReporter] Telegram send: {e}")

    # ── Redis publish ─────────────────────────────────────────────────────────

    def _publish_redis(self, signal: "Signal") -> None:
        if not self._pub:
            return
        try:
            journal_payload = signal.journal.to_dict(signal)
            event = {
                "type":    "SIGNAL_JOURNAL_UPDATE",
                "asset":   signal.asset,
                "direction": signal.direction,
                "alive":   signal.alive,
                "journal": journal_payload,
                "ts":      int(time.time() * 1000),
            }
            self._pub.publish("SIGNAL_JOURNAL_UPDATE", json.dumps(event, default=str))
            self._pub.lpush("SIGNAL_JOURNAL_LOG", json.dumps(journal_payload, default=str))
            self._pub.ltrim("SIGNAL_JOURNAL_LOG", 0, 99)
        except Exception as e:
            logger.debug(f"[SignalReporter] Redis publish: {e}")

    # ── Daily optimiser (Option C) ────────────────────────────────────────────

    def _start_daily_optimiser(self) -> None:
        # FIX Race1: The DailyOptimiser previously started at import time
        # (T≈0.05s) — before init_db(), before the engine singleton, before
        # Telegram was wired.  If the bot restarted near 3AM the first run
        # would fire within minutes with no fetcher → silent skip.
        # Now the thread only starts after wire_telegram() is called, which
        # happens at T≈130s in bot.py — well after all dependencies are ready.
        if self._daily_thread and self._daily_thread.is_alive():
            return  # already running
        self._daily_thread = threading.Thread(
            target=self._daily_optimise_loop,
            name="DailyOptimiser",
            daemon=True,
        )
        self._daily_thread.start()
        logger.info("[SignalReporter] DailyOptimiser thread started")

    def _daily_optimise_loop(self) -> None:
        """Run once daily at DAILY_OPTIMISE_HOUR UTC."""
        import datetime
        last_run_date = None

        while True:
            try:
                now  = datetime.datetime.utcnow()
                today = now.date()
                if now.hour == DAILY_OPTIMISE_HOUR and last_run_date != today:
                    last_run_date = today
                    logger.info("[SignalReporter] Daily optimisation starting...")
                    self._run_daily_optimisation()
                    logger.info("[SignalReporter] Daily optimisation complete")
            except Exception as e:
                logger.error(f"[SignalReporter] Daily optimiser error: {e}")
            time.sleep(1800)   # check every 30 minutes

    def _run_daily_optimisation(self) -> None:
        """
        For each asset, run a small parameter grid search and store the
        best config. Results are displayed on the dashboard.
        """
        try:
            from strategy_lab import optimize_strategy, StrategyBuilder
            from config.config import ASSET_CATEGORIES

            for category, assets in ASSET_CATEGORIES.items():
                for asset in assets[:3]:   # limit to first 3 per category
                    try:
                        results = optimize_strategy(
                            base_config=StrategyBuilder.example_config(),
                            param_grid={
                                "rsi_period": [10, 14, 21],
                                "stop_mult":  [1.0, 1.5, 2.0],
                            },
                            asset=asset,
                            category=category,
                            periods=300,
                        )
                        if results:
                            self._store_optimisation_result(asset, category, results[0])
                            # Invalidate cache so next signal gets fresh backtest
                            with _cache_lock:
                                _backtest_cache.pop(f"{asset}:{category}", None)
                    except Exception as e:
                        logger.debug(f"[SignalReporter] Optimise {asset}: {e}")
                    time.sleep(2)   # gentle pacing between assets
        except Exception as e:
            logger.error(f"[SignalReporter] _run_daily_optimisation: {e}")

    def _store_optimisation_result(self, asset: str, category: str, result: Dict) -> None:
        if not self._db_ok:
            return
        try:
            from services.db_pool import get_db
            get_db().save_strategy_optimisation_result(asset, category, result)
        except Exception as e:
            logger.debug(f"[SignalReporter] store_optimisation: {e}")

    # ── Internal setup ────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client as _get_redis_client
            self._pub = _get_redis_client()
            if not self._pub:
                raise RuntimeError("Redis pool unavailable")
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[SignalReporter] Redis unavailable: {e}")

    def _init_db(self) -> None:
        try:
            from services.db_pool import get_db
            get_db().ensure_strategy_reporting_tables()
            self._db_ok = True
            logger.info("[SignalReporter] DB tables ready")
        except Exception as e:
            logger.warning(f"[SignalReporter] DB unavailable ({e}) — using memory only")

    def _extract_strategy_performance(self, signal: "Signal") -> Optional[Dict[str, float]]:
        validation_entry = next(
            (entry for entry in signal.journal.entries if entry.name == "research_validation"),
            None,
        )
        if validation_entry:
            data = validation_entry.data or {}
            live_total = int(data.get("live_validation_total", 0) or 0)
            live_accuracy = float(data.get("live_validation_accuracy_pct", 0.0) or 0.0) / 100.0
            holdout_accuracy = float(data.get("holdout_accuracy", 0.0) or 0.0)
            walk_forward_accuracy = float(data.get("walk_forward_accuracy", 0.0) or 0.0)
            derived_win_rate = live_accuracy if live_total > 0 else max(holdout_accuracy, walk_forward_accuracy)
            return {
                "win_rate": max(0.0, min(1.0, derived_win_rate)),
                "sharpe_ratio": float(data.get("sharpe", 0.0) or 0.0),
                "total_trades": max(0, live_total),
            }

        backtest_entry = next(
            (entry for entry in signal.journal.entries if entry.name == "backtest"),
            None,
        )
        if backtest_entry:
            data = backtest_entry.data or {}
            return {
                "win_rate": float(data.get("win_rate", 0.0) or 0.0),
                "sharpe_ratio": float(data.get("sharpe", 0.0) or 0.0),
                "total_trades": int(data.get("trades", 0) or 0),
            }

        return None


# ── Module-level singleton ────────────────────────────────────────────────────
reporter = SignalReporter()
