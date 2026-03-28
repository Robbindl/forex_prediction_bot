from __future__ import annotations

# ── DDL for strategy tables ───────────────────────────────────────────────────

_CREATE_STRATEGY_PERFORMANCE = """
CREATE TABLE IF NOT EXISTS strategy_performance (
    asset        TEXT NOT NULL,
    category     TEXT NOT NULL,
    strategy_id  TEXT NOT NULL,
    win_rate     REAL NOT NULL DEFAULT 0,
    sharpe_ratio REAL NOT NULL DEFAULT 0,
    total_trades INT  NOT NULL DEFAULT 0,
    recorded_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset, strategy_id)
);
"""

_CREATE_STRATEGY_OPTIMISATION = """
CREATE TABLE IF NOT EXISTS strategy_optimisation (
    asset         TEXT NOT NULL,
    category      TEXT NOT NULL,
    best_params   TEXT NOT NULL DEFAULT '{}',
    sharpe        REAL NOT NULL DEFAULT 0,
    win_rate      REAL NOT NULL DEFAULT 0,
    optimised_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (asset, category)
);
"""

import json
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from config.config import MIN_FINAL_CONFIDENCE
from config.config import MIN_FINAL_CONFIDENCE
from sqlalchemy import text
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
# Layer 7 floor is 0.55 — signals between 0.55-0.62 survive the pipeline
# but are skipped by the trading loop, so no need to alert on them.

# ── Backtest cache (avoids re-running for same asset repeatedly) ──────────────
_backtest_cache:    Dict[str, dict] = {}   # asset → {result, ts}
_cache_ttl_secs    = 3600                  # cache results for 1 hour
_cache_lock        = threading.Lock()


class PipelineReporter:
    """
    Singleton post-pipeline reporter.
    Wire into pipeline.py — call report() after every pipeline.run().
    """

    _instance: Optional["PipelineReporter"] = None
    _lock      = threading.Lock()

    def __new__(cls) -> "PipelineReporter":
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
        logger.info("[PipelineReporter] Initialised")

    # ── Public API ────────────────────────────────────────────────────────────

    def wire_telegram(self, telegram_bot) -> None:
        """Call from bot.py after Telegram is started."""
        self._telegram = telegram_bot
        logger.info("[PipelineReporter] Telegram wired")
        # FIX Race1: start DailyOptimiser here, after engine + Telegram ready
        self._start_daily_optimiser()

    def report(self, signal: "Signal", context: Dict[str, Any]) -> "Signal":
        """
        Called by pipeline.py after every pipeline.run().
        Works for both surviving AND killed signals.
        Returns the (possibly confidence-adjusted) signal.
        """
        try:
            # 1. Run auto-backtest and adjust confidence
            signal = self._run_backtest(signal)

            # 2. Store performance in DB
            self._store_performance(signal)

            # 3. Send Telegram journal report
            self._send_telegram(signal)

            # 4. Publish to Redis for dashboard
            self._publish_redis(signal)

        except Exception as e:
            logger.error(f"[PipelineReporter] report() error: {e}", exc_info=True)

        return signal

    # ── Auto-backtest (Option A) ───────────────────────────────────────────────

    def _run_backtest(self, signal: "Signal") -> "Signal":
        """
        Policy-only operation: strategy lab backtest adjustment is disabled.
        """
        signal.journal.record(
            layer=0, name="policy_backtest", decision=INFO,
            reason="strategy lab backtest adjustment is disabled in policy-only mode",
            conf_before=signal.confidence,
            conf_after=signal.confidence,
            data={},
        )
        return signal

    # ── Performance storage (Option C) ───────────────────────────────────────

    def _store_performance(self, signal: "Signal") -> None:
        """Store backtest result and signal outcome in DB."""
        if not self._db_ok:
            return
        try:
            from services.db_pool import get_db
            from sqlalchemy import text
            backtest_entry = next(
                (e for e in signal.journal.entries if e.name == "backtest"),
                None
            )
            if not backtest_entry:
                return
            sql = text("""
                INSERT INTO strategy_performance
                    (asset, category, strategy_id, win_rate, sharpe_ratio,
                     total_trades, recorded_at)
                VALUES (:asset, :category, :strategy_id, :win_rate, :sharpe_ratio,
                        :total_trades, NOW())
                ON CONFLICT (asset, strategy_id)
                DO UPDATE SET
                    win_rate     = EXCLUDED.win_rate,
                    sharpe_ratio = EXCLUDED.sharpe_ratio,
                    total_trades = EXCLUDED.total_trades,
                    recorded_at  = NOW();
            """)
            data = backtest_entry.data
            db = get_db()
            with db.get_session() as s:
                s.execute(sql, {
                    "asset": signal.canonical_asset or signal.asset,
                    "category": signal.category,
                    "strategy_id": signal.strategy_id or "voting",
                    "win_rate": data.get("win_rate", 0.0),
                    "sharpe_ratio": data.get("sharpe", 0.0),
                    "total_trades": data.get("trades", 0),
                })
        except Exception as e:
            logger.debug(f"[PipelineReporter] store_performance: {e}")

    # ── Telegram (Option A) ───────────────────────────────────────────────────

    def _send_telegram(self, signal: "Signal") -> None:
        if not self._telegram:
            return
        if not signal.alive:
            logger.debug(f"[PipelineReporter] Skipping Telegram for dead signal {signal.asset} {signal.direction}")
            return

        if signal.confidence < TELEGRAM_SIGNAL_MIN_CONFIDENCE:
            logger.debug(
                f"[PipelineReporter] Skipping Telegram for {signal.asset} due to low final confidence "
                f"({signal.confidence:.3f} < {TELEGRAM_SIGNAL_MIN_CONFIDENCE})"
            )
            return

        asset_key = signal.canonical_asset or signal.asset
        now = time.time()
        last = self._last_telegram_sent.get(asset_key, 0.0)
        if now - last < TELEGRAM_ASSET_ALERT_COOLDOWN_SECS:
            logger.debug(
                f"[PipelineReporter] Skipping Telegram for {asset_key} due to dedupe cooldown "
                f"({now-last:.0f}s < {TELEGRAM_ASSET_ALERT_COOLDOWN_SECS}s)"
            )
            return

        try:
            msg = signal.journal.to_telegram(signal)
            logger.info(f"[PipelineReporter] Sending Telegram alert: {signal.journal.final_decision()} {signal.asset} {signal.direction}")
            self._telegram.send_message(msg)
            logger.info(f"[PipelineReporter] Telegram sent for {signal.asset} {signal.direction}")
            self._last_telegram_sent[asset_key] = now
        except Exception as e:
            logger.debug(f"[PipelineReporter] Telegram send: {e}")

    # ── Redis publish ─────────────────────────────────────────────────────────

    def _publish_redis(self, signal: "Signal") -> None:
        if not self._pub:
            return
        try:
            journal_payload = signal.journal.to_dict()
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
            logger.debug(f"[PipelineReporter] Redis publish: {e}")

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
        logger.info("[PipelineReporter] DailyOptimiser thread started")

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
                    logger.info("[PipelineReporter] Daily optimisation starting...")
                    self._run_daily_optimisation()
                    logger.info("[PipelineReporter] Daily optimisation complete")
            except Exception as e:
                logger.error(f"[PipelineReporter] Daily optimiser error: {e}")
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
                        logger.debug(f"[PipelineReporter] Optimise {asset}: {e}")
                    time.sleep(2)   # gentle pacing between assets
        except Exception as e:
            logger.error(f"[PipelineReporter] _run_daily_optimisation: {e}")

    def _store_optimisation_result(self, asset: str, category: str, result: Dict) -> None:
        if not self._db_ok:
            return
        try:
            from services.db_pool import get_db
            from sqlalchemy import text
            sql = text("""
                INSERT INTO strategy_optimisation
                    (asset, category, best_params, sharpe, win_rate, optimised_at)
                VALUES (:asset, :category, :best_params, :sharpe, :win_rate, NOW())
                ON CONFLICT (asset, category)
                DO UPDATE SET
                    best_params  = EXCLUDED.best_params,
                    sharpe       = EXCLUDED.sharpe,
                    win_rate     = EXCLUDED.win_rate,
                    optimised_at = NOW();
            """)
            db = get_db()
            with db.get_session() as s:
                s.execute(sql, {
                    "asset": asset,
                    "category": category,
                    "best_params": json.dumps(result),
                    "sharpe": result.get("sharpe", 0.0),
                    "win_rate": result.get("win_rate", 0.0),
                })
        except Exception as e:
            logger.debug(f"[PipelineReporter] store_optimisation: {e}")

    # ── Internal setup ────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            from services.redis_pool import get_client as _get_redis_client
            self._pub = _get_redis_client()
            if not self._pub:
                raise RuntimeError("Redis pool unavailable")
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[PipelineReporter] Redis unavailable: {e}")

    def _init_db(self) -> None:
        try:
            from services.db_pool import get_db
            db = get_db()
            with db.get_session() as s:
                s.execute(text("""
                    CREATE TABLE IF NOT EXISTS strategy_performance (
                        asset        TEXT NOT NULL,
                        category     TEXT NOT NULL,
                        strategy_id  TEXT NOT NULL,
                        win_rate     REAL NOT NULL DEFAULT 0,
                        sharpe_ratio REAL NOT NULL DEFAULT 0,
                        total_trades INT  NOT NULL DEFAULT 0,
                        recorded_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (asset, strategy_id)
                    );
                """))
                s.execute(text("""
                    CREATE TABLE IF NOT EXISTS strategy_optimisation (
                        asset         TEXT NOT NULL,
                        category      TEXT NOT NULL,
                        best_params   TEXT NOT NULL DEFAULT '{}',
                        sharpe        REAL NOT NULL DEFAULT 0,
                        win_rate      REAL NOT NULL DEFAULT 0,
                        optimised_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (asset, category)
                    );
                """))
            self._db_ok = True
            logger.info("[PipelineReporter] DB tables ready")
        except Exception as e:
            logger.warning(f"[PipelineReporter] DB unavailable ({e}) — using memory only")


# ── Module-level singleton ────────────────────────────────────────────────────
reporter = PipelineReporter()
