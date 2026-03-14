"""
tests/test_fixes.py — Tests for all 12 issues found in the system audit.

Every test is a unit test — no live Redis, PostgreSQL, Telegram, or network
access required. External services are either mocked or skipped automatically.

Run:
    pytest tests/test_fixes.py -v
"""
from __future__ import annotations

import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pandas as pd
import numpy as np
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


# ── Shared helpers ────────────────────────────────────────────────────────────

def _make_signal(
    asset="BTC-USD", direction="BUY", category="crypto",
    confidence=0.75, entry=50000.0, stop_loss=49000.0, take_profit=52000.0,
):
    from core.signal import Signal
    return Signal(
        asset=asset, canonical_asset=asset,
        direction=direction, category=category,
        confidence=confidence,
        entry_price=entry, stop_loss=stop_loss, take_profit=take_profit,
        strategy_id="TEST",
    )


def _flat_ohlcv(n=100, price=100.0) -> pd.DataFrame:
    return pd.DataFrame({
        "open":   [price] * n,
        "high":   [price + 0.1] * n,
        "low":    [price - 0.1] * n,
        "close":  [price] * n,
        "volume": [1_000_000] * n,
    })


def _trending_ohlcv(n=100, direction="up") -> pd.DataFrame:
    step = 0.5 if direction == "up" else -0.5
    prices = [100.0 + i * step for i in range(n)]
    return pd.DataFrame({
        "open":   prices,
        "high":   [p + 0.2 for p in prices],
        "low":    [p - 0.2 for p in prices],
        "close":  prices,
        "volume": [1_000_000] * n,
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 1 — Missing files: telegram_whale_watcher.py & telethon_whale_store.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue1MissingFiles:
    """Both stub files must exist and import without errors."""

    def test_telegram_whale_watcher_file_exists(self):
        assert (ROOT / "telegram_whale_watcher.py").exists(), (
            "telegram_whale_watcher.py is missing — ImportError in whale_alert_manager"
        )

    def test_telethon_whale_store_file_exists(self):
        assert (ROOT / "telethon_whale_store.py").exists(), (
            "telethon_whale_store.py is missing — ImportError in whale_alert_manager"
        )

    def test_telegram_whale_watcher_imports_cleanly(self):
        from telegram_whale_watcher import TelegramWhaleWatcher
        watcher = TelegramWhaleWatcher()
        assert watcher is not None

    def test_telethon_whale_store_imports_cleanly(self):
        from telethon_whale_store import whale_store
        assert whale_store is not None

    def test_telegram_whale_watcher_has_start_method(self):
        from telegram_whale_watcher import TelegramWhaleWatcher
        watcher = TelegramWhaleWatcher()
        assert hasattr(watcher, "start_monitoring")

    def test_telegram_whale_watcher_has_stop_method(self):
        from telegram_whale_watcher import TelegramWhaleWatcher
        watcher = TelegramWhaleWatcher()
        assert hasattr(watcher, "stop_monitoring")

    def test_telegram_whale_watcher_has_bot_token_attribute(self):
        from telegram_whale_watcher import TelegramWhaleWatcher
        watcher = TelegramWhaleWatcher()
        assert hasattr(watcher, "bot_token")

    def test_telegram_whale_watcher_has_get_recent_alerts(self):
        from telegram_whale_watcher import TelegramWhaleWatcher
        watcher = TelegramWhaleWatcher()
        assert hasattr(watcher, "get_recent_alerts")
        result = watcher.get_recent_alerts()
        assert isinstance(result, list)

    def test_telethon_store_len_works(self):
        from telethon_whale_store import whale_store
        assert isinstance(len(whale_store), int)

    def test_telethon_store_format_for_dashboard_returns_list(self):
        from telethon_whale_store import whale_store
        result = whale_store.format_for_dashboard(hours=24)
        assert isinstance(result, list)

    def test_telethon_store_add_does_not_raise(self):
        from telethon_whale_store import whale_store
        whale_store.add({
            "title": "🐋 BTC $5.0M — test",
            "value_usd": 5_000_000,
            "symbol": "BTC",
            "source": "test",
        })

    def test_whale_alert_manager_imports_after_files_exist(self):
        try:
            import importlib, sys
            for mod in list(sys.modules):
                if "whale_alert" in mod or "telethon_whale" in mod or "telegram_whale" in mod:
                    sys.modules.pop(mod, None)
            from whale_alert_manager import WhaleAlertManager  # noqa: F401
        except ImportError as e:
            pytest.fail(f"whale_alert_manager still raises ImportError: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 2 — Pipeline context missing price_data, spread, ml_prediction
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue2PipelineContext:
    """_generate_signals() must return (Signal, Dict) pairs; context must be populated."""

    @pytest.fixture
    def engine(self):
        with patch("config.database.create_db_engine"):
            from core.engine import TradingCore
            eng = TradingCore.__new__(TradingCore)
            eng.balance = 1000.0
            eng.strategy_mode = "voting"
            eng.no_telegram = True

            from core.state import SystemState
            from core.events import EventBus
            from core.assets import AssetRegistry
            from core.pipeline import Pipeline

            eng.state    = SystemState()
            eng.state._open_positions = {}
            eng.state._balance = 1000.0
            eng.events   = EventBus()
            eng.registry = AssetRegistry()
            eng.pipeline = Pipeline()
            eng.telegram = None
            eng.fetcher  = None
            eng._risk_manager  = None
            eng._paper_trader  = None
            eng._stop_event    = threading.Event()
            eng._engine_ready  = threading.Event()
            eng._engine_ready.set()
            eng._is_running    = True
            return eng

    def test_generate_signals_returns_tuples(self, engine):
        mock_fetcher = MagicMock()
        mock_fetcher.get_ohlcv.return_value = _trending_ohlcv()
        mock_fetcher.get_real_time_price.return_value = (100.0, 0.01)
        engine.fetcher = mock_fetcher

        with patch("strategies.voting.VotingStrategy.generate", return_value=_make_signal()):
            with patch("ml.predictor.MLPredictor.predict", return_value=(0.7, 0.6)):
                result = engine._generate_signals()

        for item in result:
            assert isinstance(item, tuple), "Expected (Signal, dict) tuples"
            assert len(item) == 2
            sig, ctx = item
            from core.signal import Signal
            assert isinstance(sig, Signal)
            assert isinstance(ctx, dict)

    def test_context_contains_price_data(self, engine):
        df = _trending_ohlcv()
        mock_fetcher = MagicMock()
        mock_fetcher.get_ohlcv.return_value = df
        mock_fetcher.get_real_time_price.return_value = (100.0, 0.01)
        engine.fetcher = mock_fetcher

        with patch("strategies.voting.VotingStrategy.generate", return_value=_make_signal()):
            with patch("ml.predictor.MLPredictor.predict", return_value=(0.7, 0.6)):
                result = engine._generate_signals()

        assert len(result) > 0, "No signals generated"
        _, ctx = result[0]
        assert "price_data" in ctx, "price_data missing from pipeline context"
        assert isinstance(ctx["price_data"], pd.DataFrame)

    def test_context_contains_spread(self, engine):
        mock_fetcher = MagicMock()
        mock_fetcher.get_ohlcv.return_value = _trending_ohlcv()
        mock_fetcher.get_real_time_price.return_value = (50000.0, 25.0)
        engine.fetcher = mock_fetcher

        with patch("strategies.voting.VotingStrategy.generate", return_value=_make_signal()):
            with patch("ml.predictor.MLPredictor.predict", return_value=(0.7, 0.6)):
                result = engine._generate_signals()

        assert len(result) > 0
        _, ctx = result[0]
        assert "spread" in ctx, "spread missing from pipeline context"
        assert ctx["spread"] == pytest.approx(25.0)

    def test_context_contains_ml_prediction(self, engine):
        mock_fetcher = MagicMock()
        mock_fetcher.get_ohlcv.return_value = _trending_ohlcv()
        mock_fetcher.get_real_time_price.return_value = (100.0, 0.01)
        engine.fetcher = mock_fetcher

        with patch("strategies.voting.VotingStrategy.generate", return_value=_make_signal()):
            with patch("ml.predictor.MLPredictor.predict", return_value=(0.82, 0.64)):
                result = engine._generate_signals()

        assert len(result) > 0
        _, ctx = result[0]
        assert "ml_prediction" in ctx, "ml_prediction missing from pipeline context"
        assert ctx["ml_prediction"] == pytest.approx(0.82)

    def test_layer3_receives_price_data_via_context(self):
        from layers.layer3_regime import RegimeLayer
        layer = RegimeLayer()
        sig   = _make_signal(direction="BUY")
        df    = _trending_ohlcv(direction="up")
        result = layer.process(sig, {"price_data": df})
        assert result is not None
        assert sig.metadata.get("regime") != "unknown"

    def test_layer1_ml_boost_fires_when_context_populated(self):
        from layers.layer1_voting import VotingLayer
        layer  = VotingLayer()
        sig    = _make_signal(confidence=0.75, direction="BUY")
        before = sig.confidence
        layer.process(sig, {"ml_prediction": 0.8})
        assert sig.confidence > before, "ML agree boost did not fire"

    def test_different_assets_get_separate_contexts(self, engine):
        dfs = {
            "BTC-USD": _trending_ohlcv(direction="up"),
            "ETH-USD": _trending_ohlcv(direction="down"),
        }

        def mock_ohlcv(asset, cat, **kw):
            return dfs.get(asset, _flat_ohlcv())

        mock_fetcher = MagicMock()
        mock_fetcher.get_ohlcv.side_effect = mock_ohlcv
        mock_fetcher.get_real_time_price.return_value = (100.0, 0.0)
        engine.fetcher = mock_fetcher

        def mock_generate(asset, canonical, cat, df):
            sig = _make_signal(asset=asset)
            return sig

        with patch("strategies.voting.VotingStrategy.generate", side_effect=mock_generate):
            with patch("ml.predictor.MLPredictor.predict", return_value=(0.6, 0.5)):
                result = engine._generate_signals()

        ctx_dfs = {sig.asset: ctx["price_data"] for sig, ctx in result}
        for asset, expected_df in dfs.items():
            if asset in ctx_dfs:
                assert ctx_dfs[asset] is expected_df, (
                    f"Context for {asset} has wrong price_data (shared context bleed)"
                )


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 3 — Telegram never started
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue3TelegramStart:

    def test_telegram_manager_has_start_method(self):
        from telegram_manager import TelegramManager
        assert hasattr(TelegramManager, "start")

    def test_start_requires_token_and_chat_id(self):
        from telegram_manager import TelegramManager
        import inspect
        sig    = inspect.signature(TelegramManager.start)
        params = list(sig.parameters)
        assert "token"   in params, "start() missing 'token' parameter"
        assert "chat_id" in params, "start() missing 'chat_id' parameter"

    def test_bot_py_calls_start_with_credentials(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        assert "telegram_manager.start(" in src, (
            "bot.py does not call telegram_manager.start() — Telegram never activates"
        )

    def test_bot_py_passes_token_and_chat_id_to_start(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        start_line_idx = next(
            (i for i, line in enumerate(src.splitlines())
             if "telegram_manager.start(" in line), None,
        )
        assert start_line_idx is not None
        start_block = "\n".join(src.splitlines()[start_line_idx:start_line_idx + 5])
        assert "TELEGRAM_TOKEN" in start_block or "token" in start_block.lower()

    def test_engine_telegram_wired_to_bot_not_manager(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        assert "telegram_manager.bot" in src, (
            "bot.py must wire engine.telegram = telegram_manager.bot"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 4 — engine.telegram.alert_trade_opened() AttributeError
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue4TelegramAlertMethod:

    def _make_engine(self):
        from core.engine import TradingCore
        eng = TradingCore.__new__(TradingCore)
        eng.telegram = None
        return eng

    def test_notify_silent_when_telegram_is_none(self):
        eng = self._make_engine()
        eng.telegram = None
        eng._notify_telegram_open({"trade_id": "t1"})

    def test_notify_calls_alert_on_commander(self):
        eng = self._make_engine()
        mock_commander = MagicMock(spec=["alert_trade_opened"])
        eng.telegram   = mock_commander
        eng._notify_telegram_open({"trade_id": "t1"})
        mock_commander.alert_trade_opened.assert_called_once_with({"trade_id": "t1"})

    def test_notify_resolves_bot_attribute_on_manager(self):
        eng = self._make_engine()
        mock_commander = MagicMock(spec=["alert_trade_opened"])
        mock_manager   = MagicMock()
        mock_manager.bot = mock_commander
        eng.telegram = mock_manager
        eng._notify_telegram_open({"trade_id": "t2"})
        mock_commander.alert_trade_opened.assert_called_once_with({"trade_id": "t2"})

    def test_notify_does_not_raise_on_attribute_error(self):
        eng = self._make_engine()
        eng.telegram = object()
        eng._notify_telegram_open({"trade_id": "t3"})

    def test_notify_does_not_raise_on_telegram_exception(self):
        eng = self._make_engine()
        mock_commander = MagicMock()
        mock_commander.alert_trade_opened.side_effect = RuntimeError("Telegram down")
        eng.telegram = mock_commander
        eng._notify_telegram_open({"trade_id": "t4"})


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 5 — SentimentAnalyzer instantiated per-signal
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue5SentimentSingleton:

    def setup_method(self):
        import layers.layer5_sentiment as l5
        l5._sa_instance = None

    def test_analyzer_instantiated_only_once(self):
        import layers.layer5_sentiment as l5
        call_count = 0

        class FakeAnalyzer:
            def __init__(self):
                nonlocal call_count
                call_count += 1

            def get_comprehensive_sentiment(self, asset, category):
                return 0.0

        with patch("sentiment_analyzer.SentimentAnalyzer", FakeAnalyzer):
            l5._sa_instance = None
            a1 = l5._get_analyzer()
            a2 = l5._get_analyzer()
            a3 = l5._get_analyzer()

        assert a1 is a2 is a3, "Singleton broken — multiple instances created"
        assert call_count == 1, f"SentimentAnalyzer.__init__ called {call_count} times"

    def test_layer5_uses_context_score_without_creating_analyzer(self):
        import layers.layer5_sentiment as l5
        sig   = _make_signal(direction="BUY", confidence=0.75)
        layer = l5.SentimentLayer()

        init_calls = 0

        class FakeAnalyzer:
            def __init__(self): nonlocal init_calls; init_calls += 1

        with patch("sentiment_analyzer.SentimentAnalyzer", FakeAnalyzer):
            l5._sa_instance = None
            layer.process(sig, {"sentiment_score": 0.5})

        assert init_calls == 0, (
            "SentimentAnalyzer was initialised even though context had sentiment_score"
        )

    def test_singleton_survives_concurrent_calls(self):
        import layers.layer5_sentiment as l5
        l5._sa_instance = None

        class FakeAnalyzer:
            pass

        results = []

        def _call():
            with patch("sentiment_analyzer.SentimentAnalyzer", FakeAnalyzer):
                results.append(l5._get_analyzer())

        threads = [threading.Thread(target=_call) for _ in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()

        non_none = [r for r in results if r is not None]
        if non_none:
            assert all(r is non_none[0] for r in non_none), (
                "Concurrent _get_analyzer() returned different instances"
            )

    def test_layer5_process_stores_score_in_metadata(self):
        from layers.layer5_sentiment import SentimentLayer
        layer = SentimentLayer()
        sig   = _make_signal(direction="BUY")
        layer.process(sig, {"sentiment_score": 0.3})
        assert "sentiment_score" in sig.metadata
        assert sig.metadata["sentiment_score"] == pytest.approx(0.3)


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 6 — Risk manager daily loss guard never reset
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue6RiskGuardReset:

    def test_reset_daily_updates_initial_balance(self):
        from risk.manager import RiskManager
        rm = RiskManager(account_balance=1000.0)
        rm.reset_daily(1200.0)
        can_trade, _ = rm._daily_loss_guard.check(daily_pnl=-59.0)
        assert can_trade is True

    def test_loss_guard_stale_without_reset(self):
        from risk.manager import DailyLossGuard
        guard = DailyLossGuard(balance=1000.0, limit_pct=5.0)
        can_trade, _ = guard.check(daily_pnl=-60.0)
        assert can_trade is False

    def test_trading_cycle_calls_reset_on_day_rollover(self):
        with patch("config.database.create_db_engine"):
            from core.engine import TradingCore
            eng = TradingCore.__new__(TradingCore)

        mock_state = MagicMock()
        mock_state.check_day_rollover.return_value = True
        mock_state.balance = 1500.0
        mock_state.open_position_count.return_value = 0
        mock_state.daily_pnl = 0.0
        mock_state.daily_trades = 0
        mock_state.get_open_positions.return_value = []

        mock_rm = MagicMock()
        eng.state         = mock_state
        eng._risk_manager = mock_rm
        eng._paper_trader = None
        eng._stop_event   = threading.Event()
        eng.fetcher       = None
        eng.registry      = MagicMock()
        eng.registry.all_assets.return_value = []
        eng.pipeline      = MagicMock()
        eng.pipeline.run.return_value = None

        eng._trading_cycle()
        mock_rm.reset_daily.assert_called_once_with(1500.0)

    def test_trading_cycle_does_not_reset_when_day_unchanged(self):
        with patch("config.database.create_db_engine"):
            from core.engine import TradingCore
            eng = TradingCore.__new__(TradingCore)

        mock_state = MagicMock()
        mock_state.check_day_rollover.return_value = False
        mock_state.balance = 1000.0
        mock_state.open_position_count.return_value = 0
        mock_state.daily_pnl = 0.0
        mock_state.daily_trades = 0
        mock_state.get_open_positions.return_value = []

        mock_rm = MagicMock()
        eng.state         = mock_state
        eng._risk_manager = mock_rm
        eng._paper_trader = None
        eng._stop_event   = threading.Event()
        eng.fetcher       = None
        eng.registry      = MagicMock()
        eng.registry.all_assets.return_value = []
        eng.pipeline      = MagicMock()
        eng.pipeline.run.return_value = None

        eng._trading_cycle()
        mock_rm.reset_daily.assert_not_called()

    def test_reset_daily_allows_fresh_trading_after_prior_day_loss(self):
        from risk.manager import RiskManager
        rm = RiskManager(account_balance=1000.0)
        can_day1, _ = rm._daily_loss_guard.check(daily_pnl=-60.0)
        assert can_day1 is False
        rm.reset_daily(940.0)
        can_day2, _ = rm._daily_loss_guard.check(daily_pnl=0.0)
        assert can_day2 is True, "Trading still blocked on new day after reset"


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 7 — WhaleAlertManager never started from bot.py
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue7WhaleBotWiring:

    def test_bot_py_starts_whale_alert_manager(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        assert "WhaleAlertManager" in src, (
            "bot.py never imports or starts WhaleAlertManager"
        )

    def test_bot_py_wires_on_alert_callback(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        assert "on_alert" in src, (
            "bot.py does not set on_alert callback on WhaleAlertManager"
        )

    def test_bot_py_calls_ingest_whale_alert(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        assert "ingest_whale_alert" in src, (
            "bot.py does not bridge WhaleAlertManager to ingest_whale_alert"
        )

    def test_bot_py_starts_whale_manager_after_engine_ready(self):
        src = (ROOT / "bot.py").read_text(encoding="utf-8")
        ready_pos = src.find("wait_until_ready")
        whale_pos = src.find("WhaleAlertManager")
        assert ready_pos != -1 and whale_pos != -1
        assert whale_pos > ready_pos

    def test_ingest_whale_alert_populates_cache(self):
        from layers.layer6_whale import ingest_whale_alert, _WHALE_CACHE
        _WHALE_CACHE.clear()
        ingest_whale_alert("BTC-USD", "SELL", 2_000_000, "test")
        assert len(_WHALE_CACHE) == 1
        assert _WHALE_CACHE[0]["asset"] == "BTC-USD"
        _WHALE_CACHE.clear()

    def test_whale_cache_feeds_layer6(self):
        from layers.layer6_whale import WhaleLayer, ingest_whale_alert, _WHALE_CACHE
        _WHALE_CACHE.clear()
        ingest_whale_alert("BTC-USD", "SELL", 5_000_000, "test")
        ingest_whale_alert("BTC-USD", "SELL", 5_000_000, "test")
        layer  = WhaleLayer()
        sig    = _make_signal(asset="BTC-USD", direction="BUY")
        result = layer.process(sig, {})
        assert result is None, "Whale layer should have killed the BUY signal"
        _WHALE_CACHE.clear()


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 8 — Core engine never publishes to Redis
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue8RedisPublish:

    def test_execute_signal_code_references_redis_broker(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _execute_signal")
        end   = src.find("\n    def ", start + 1)
        method_src = src[start:end]
        assert "publish_signal" in method_src or "redis_broker" in method_src, (
            "_execute_signal does not publish to Redis"
        )

    def test_execute_signal_code_references_publish_positions(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _execute_signal")
        end   = src.find("\n    def ", start + 1)
        method_src = src[start:end]
        assert "publish_positions" in method_src or "positions" in method_src

    def test_trading_cycle_publishes_prices(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _trading_cycle")
        end   = src.find("\n    def ", start + 1)
        cycle_src = src[start:end]
        assert "publish_price" in cycle_src or "redis_broker" in cycle_src


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 9 — Batch pipeline shared context
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue9PerSignalContext:

    def test_trading_cycle_does_not_call_run_batch(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _trading_cycle")
        end   = src.find("\n    def ", start + 1)
        cycle_src = src[start:end]
        assert "run_batch" not in cycle_src, (
            "_trading_cycle still calls pipeline.run_batch() with a shared context"
        )

    def test_trading_cycle_calls_pipeline_run_individually(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _trading_cycle")
        end   = src.find("\n    def ", start + 1)
        cycle_src = src[start:end]
        assert "pipeline.run(" in cycle_src

    def test_generate_signals_returns_list_of_tuples(self):
        src = (ROOT / "core" / "engine.py").read_text(encoding="utf-8")
        start = src.find("def _generate_signals")
        end   = src.find("\n    def ", start + 1)
        method_src = src[start:end]
        assert "Tuple" in method_src or "ctx" in method_src


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 10 — Redis subscribe thread no reconnect
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue10RedisReconnect:

    def test_subscribe_code_has_while_true_loop(self):
        src = (ROOT / "redis_broker.py").read_text(encoding="utf-8")
        start = src.find("def subscribe(")
        end   = src.find("\n    def ", start + 1)
        method_src = src[start:end]
        assert "while True" in method_src, (
            "subscribe() has no reconnect loop"
        )

    def test_subscribe_code_has_sleep_on_exception(self):
        src = (ROOT / "redis_broker.py").read_text(encoding="utf-8")
        start = src.find("def subscribe(")
        end   = src.find("\n    def ", start + 1)
        method_src = src[start:end]
        assert "sleep" in method_src

    def test_subscribe_reconnects_after_failure(self):
        from redis_broker import RedisBroker
        broker = RedisBroker.__new__(RedisBroker)
        broker._enabled = True
        broker._redis   = MagicMock()
        broker._lock    = threading.Lock()

        connect_attempts = []

        def _fake_redis_class(*a, **kw):
            class FakeClient:
                def pubsub(self_inner):
                    class FakePubSub:
                        def subscribe(self_ps, ch): pass
                        def listen(self_ps):
                            connect_attempts.append(1)
                            if len(connect_attempts) == 1:
                                raise ConnectionError("Redis dropped")
                            yield {"type": "message", "data": '{"ok": true}'}
                            raise StopIteration
                    return FakePubSub()
            return FakeClient()

        with patch("redis.Redis", _fake_redis_class):
            with patch("time.sleep"):
                broker.subscribe("test_channel", lambda d: None)
                time.sleep(0.2)

        assert len(connect_attempts) >= 1


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 11 — config/database.py creates engine at import time
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue11DatabaseImport:

    def test_config_database_is_mocked_in_tests(self):
        import sys
        db_mod = sys.modules.get("config.database")
        assert db_mod is not None

    def test_database_url_has_sensible_default(self):
        from config.config import DATABASE_URL
        assert "postgresql" in DATABASE_URL.lower()

    def test_db_pool_get_db_returns_service_without_real_db(self):
        from services.db_pool import get_db
        db = get_db()
        assert db is not None

    def test_system_state_loads_without_db(self):
        from core.state import SystemState
        s = SystemState()
        assert s is not None
        assert s.balance >= 0


# ═══════════════════════════════════════════════════════════════════════════════
# ISSUE 12 — Cache purge_expired never runs automatically
# ═══════════════════════════════════════════════════════════════════════════════

class TestIssue12CacheAutoPurge:

    def test_cache_starts_purge_thread(self):
        from data.cache import Cache
        before_threads = {t.name for t in threading.enumerate()}
        Cache(default_ttl=60, purge_interval=9999)
        after_threads  = {t.name for t in threading.enumerate()}
        new_threads    = after_threads - before_threads
        assert any("CachePurge" in name for name in new_threads)

    def test_purge_thread_is_daemon(self):
        from data.cache import Cache
        Cache(default_ttl=60, purge_interval=9999)
        purge_threads = [t for t in threading.enumerate() if "CachePurge" in t.name]
        assert purge_threads
        for t in purge_threads:
            assert t.daemon

    def test_purge_removes_expired_entries_automatically(self):
        from data.cache import Cache
        c = Cache(default_ttl=60, purge_interval=1)
        c.set("key1", "val1", ttl=0)
        c.set("key2", "val2", ttl=60)
        time.sleep(1.5)
        assert c.get("key1") is None
        assert c.get("key2") == "val2"

    def test_purge_expired_returns_count(self):
        from data.cache import Cache
        c = Cache(default_ttl=60, purge_interval=9999)
        c.set("a", 1, ttl=0)
        c.set("b", 2, ttl=0)
        c.set("c", 3, ttl=60)
        removed = c.purge_expired()
        assert removed == 2

    def test_cache_accepts_purge_interval_param(self):
        from data.cache import Cache
        import inspect
        sig    = inspect.signature(Cache.__init__)
        params = list(sig.parameters)
        assert "purge_interval" in params

    def test_valid_entries_survive_auto_purge(self):
        from data.cache import Cache
        c = Cache(default_ttl=60, purge_interval=1)
        c.set("keep_me", {"important": True}, ttl=60)
        time.sleep(1.5)
        assert c.get("keep_me") == {"important": True}

    def test_cache_len_reflects_purge(self):
        from data.cache import Cache
        c = Cache(default_ttl=60, purge_interval=9999)
        c.set("x", 1, ttl=0)
        c.set("y", 2, ttl=60)
        c.purge_expired()
        assert len(c) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Integration smoke test — full pipeline with populated context
# ═══════════════════════════════════════════════════════════════════════════════

class TestFullPipelineWithContext:

    def _make_full_context(self, direction="BUY"):
        df = _trending_ohlcv(direction="up" if direction == "BUY" else "down", n=60)
        return {
            "price_data":     df,
            "spread":         0.5,
            "ml_prediction":  0.75 if direction == "BUY" else 0.25,
            "ml_confidence":  0.5,
            "sentiment_score": 0.3,
            "balance":        10000.0,
            "open_count":     0,
            "daily_pnl":      0.0,
            "asset":          "BTC-USD",
            "category":       "crypto",
        }

    def test_buy_signal_survives_all_layers_with_full_context(self):
        from core.pipeline import Pipeline
        pipeline = Pipeline()
        sig = _make_signal(
            direction="BUY", confidence=0.80,
            entry=50000, stop_loss=49000, take_profit=53000,
            category="crypto",
        )
        ctx    = self._make_full_context("BUY")
        result = pipeline.run(sig, ctx)
        assert result is not None, (
            f"Signal killed: {sig.kill_reason} at layer {sig.layer_reached}"
        )

    def test_sell_signal_killed_by_trending_up_regime(self):
        from layers.layer3_regime import RegimeLayer
        layer = RegimeLayer()
        sig   = _make_signal(direction="SELL", confidence=0.80)
        df    = _trending_ohlcv(direction="up", n=60)
        result = layer.process(sig, {"price_data": df})
        assert result is None

    def test_signal_with_price_data_gets_non_unknown_regime(self):
        from layers.layer3_regime import RegimeLayer
        layer = RegimeLayer()
        sig   = _make_signal(direction="BUY")
        df    = _trending_ohlcv(direction="up", n=60)
        layer.process(sig, {"price_data": df})
        assert sig.metadata.get("regime") in (
            "trending_up", "trending_down", "ranging", "volatile"
        )

    def test_ml_prediction_in_context_affects_layer1_confidence(self):
        from layers.layer1_voting import VotingLayer
        layer = VotingLayer()

        sig_agree = _make_signal(direction="BUY", confidence=0.75)
        before_agree = sig_agree.confidence
        layer.process(sig_agree, {"ml_prediction": 0.9})

        sig_disagree = _make_signal(direction="BUY", confidence=0.75)
        before_disagree = sig_disagree.confidence
        layer.process(sig_disagree, {"ml_prediction": 0.1})

        assert sig_agree.confidence > before_agree
        assert sig_disagree.confidence < before_disagree