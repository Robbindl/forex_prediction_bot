"""
core/pipeline_reporter.py — Post-pipeline reporter.

After every signal completes the 7-layer pipeline (pass or fail),
this module:

  1. Runs an auto-backtest on the signal's asset using the active strategy
     (Option A) — adjusts confidence up/down based on historical win rate,
     adds backtest entry to the Signal Journal.

  2. Stores performance data in the database (Option C) — every backtest
     result is persisted so the dashboard can show live strategy stats.

  3. Runs a daily background optimiser (Option C) — finds best parameters
     per asset automatically, stores results in DB.

  4. Sends the full Signal Journal to Telegram — every stage's decision
     in one formatted message, including the backtest result.

  5. Publishes SIGNAL_JOURNAL_UPDATE to Redis — for the dashboard live feed.

Confidence adjustment rules
----------------------------
    win_rate >= 0.65  → boost  +0.04 (strong historical edge)
    win_rate >= 0.55  → boost  +0.02 (positive edge)
    win_rate >= 0.50  → no change
    win_rate <  0.50  → reduce −0.03 (weak historical)
    win_rate <  0.40  → reduce −0.06 (poor historical — warn on Telegram)
    trades   <  10    → no adjustment (insufficient data)

The pipeline still makes the final decision via Layer 7's confidence
floor — the backtest adjustment just nudges the confidence. It never
outright blocks a signal.
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

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
TELEGRAM_SIGNAL_MIN_CONFIDENCE = 0.7  # minimum confidence to send Telegram

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
        self._start_daily_optimiser()
        logger.info("[PipelineReporter] Initialised")

    # ── Public API ────────────────────────────────────────────────────────────

    def wire_telegram(self, telegram_bot) -> None:
        """Call from bot.py after Telegram is started."""
        self._telegram = telegram_bot
        logger.info("[PipelineReporter] Telegram wired")

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
        Run (or retrieve cached) backtest for this asset.
        Adjust confidence and add INFO entry to journal.
        """
        asset    = signal.canonical_asset or signal.asset
        category = signal.category

        result = self._get_backtest_result(asset, category)
        if result is None:
            signal.journal.record(
                layer=0, name="backtest", decision=INFO,
                reason="no historical data available",
                conf_before=signal.confidence,
                conf_after=signal.confidence,
                data={},
            )
            return signal

        trades   = result.get("total_trades",  0)
        win_rate = result.get("win_rate",       0.0)
        sharpe   = result.get("sharpe_ratio",   0.0)
        pnl_pct  = result.get("total_pnl_pct",  0.0)

        conf_before = signal.confidence

        if trades >= MIN_TRADES_FOR_ADJUSTMENT:
            if win_rate >= STRONG_EDGE_THRESHOLD:
                signal.boost(0.04)
                adj = f"+0.04 boost (strong edge)"
            elif win_rate >= POSITIVE_EDGE_THRESHOLD:
                signal.boost(0.02)
                adj = f"+0.02 boost (positive edge)"
            elif win_rate < POOR_EDGE_THRESHOLD:
                signal.reduce(0.06)
                adj = f"-0.06 reduce (poor history ⚠️)"
            elif win_rate < WEAK_EDGE_THRESHOLD:
                signal.reduce(0.03)
                adj = f"-0.03 reduce (weak history)"
            else:
                adj = "no adjustment"
        else:
            adj = f"insufficient data ({trades} trades)"

        warn = " ⚠️ LOW WIN RATE" if win_rate < POOR_EDGE_THRESHOLD and trades >= MIN_TRADES_FOR_ADJUSTMENT else ""

        signal.journal.record(
            layer=0,
            name="backtest",
            decision=INFO,
            reason=f"winrate={win_rate:.0%}  Sharpe={sharpe:.2f}  trades={trades}  {adj}{warn}",
            conf_before=conf_before,
            conf_after=signal.confidence,
            data={
                "win_rate":     round(win_rate, 4),
                "sharpe":       round(sharpe,   2),
                "trades":       trades,
                "pnl_pct":      round(pnl_pct,  4),
            },
        )
        return signal

    def _get_backtest_result(self, asset: str, category: str) -> Optional[Dict]:
        """Return cached result or run a fresh backtest."""
        cache_key = f"{asset}:{category}"
        now       = time.time()

        with _cache_lock:
            cached = _backtest_cache.get(cache_key)
            if cached and (now - cached["ts"]) < _cache_ttl_secs:
                return cached["result"]

        result = self._run_fresh_backtest(asset, category)
        if result:
            with _cache_lock:
                _backtest_cache[cache_key] = {"result": result, "ts": now}
        return result

    @staticmethod
    def _run_fresh_backtest(asset: str, category: str) -> Optional[Dict]:
        """Run a quick backtest using the best known config for this asset."""
        try:
            from strategy_lab import run_backtest, StrategyBuilder
            from strategy_lab.strategy_adapter import StrategyAdapter
            from strategies.voting import VotingStrategy
            from data.fetcher import DataFetcher

            fetcher = DataFetcher()
            try:
                from config.config import TRADING_TIMEFRAME as _TF
            except Exception:
                _TF = "15m"
            _periods = {"15m": 500, "1h": 300, "4h": 200, "1d": 300}.get(_TF, 300)
            df      = fetcher.get_ohlcv(asset, category, _TF, _periods)
            if df is None or df.empty:
                return None

            # Use VotingStrategy (the live strategy) via adapter
            adapter = StrategyAdapter(VotingStrategy(), asset=asset, category=category)
            from strategy_lab.backtest_engine_v2 import BacktestEngineV2
            engine  = BacktestEngineV2(strategy=adapter, initial_balance=10_000)
            result  = engine.run(df)
            return result.to_dict()

        except Exception as e:
            logger.debug(f"[PipelineReporter] Backtest {asset}: {e}")
            return None

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
            event = {
                "type":    "SIGNAL_JOURNAL_UPDATE",
                "asset":   signal.asset,
                "direction": signal.direction,
                "alive":   signal.alive,
                "journal": signal.journal.to_dict(),
                "ts":      int(time.time() * 1000),
            }
            self._pub.publish("SIGNAL_JOURNAL_UPDATE", json.dumps(event, default=str))
        except Exception as e:
            logger.debug(f"[PipelineReporter] Redis publish: {e}")

    # ── Daily optimiser (Option C) ────────────────────────────────────────────

    def _start_daily_optimiser(self) -> None:
        self._daily_thread = threading.Thread(
            target=self._daily_optimise_loop,
            name="DailyOptimiser",
            daemon=True,
        )
        self._daily_thread.start()

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
            import redis
            from config.config import REDIS_URL
            self._pub = redis.from_url(REDIS_URL)
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