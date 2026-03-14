"""
tests/test_architecture.py — Tests for the new architecture components.
All unit tests — no real exchanges, no Redis, no running services.
"""
from __future__ import annotations
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


# ── CircuitBreaker ────────────────────────────────────────────────────────────

class TestCircuitBreaker:
    from execution.exchange_adapter import CircuitBreaker

    def test_starts_closed(self):
        from execution.exchange_adapter import CircuitBreaker
        cb = CircuitBreaker(max_failures=3)
        assert cb.is_open is False

    def test_opens_after_max_failures(self):
        from execution.exchange_adapter import CircuitBreaker
        cb = CircuitBreaker(max_failures=3)
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False
        cb.record_failure()
        assert cb.is_open is True

    def test_success_resets_failures(self):
        from execution.exchange_adapter import CircuitBreaker
        cb = CircuitBreaker(max_failures=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()
        cb.record_failure()
        cb.record_failure()
        assert cb.is_open is False

    def test_half_open_after_timeout(self):
        import time
        from execution.exchange_adapter import CircuitBreaker
        cb = CircuitBreaker(max_failures=1, reset_timeout=0.05)
        cb.record_failure()
        assert cb.is_open is True
        time.sleep(0.1)
        assert cb.is_open is False


# ── RateLimiter ───────────────────────────────────────────────────────────────

class TestRateLimiter:

    def test_allows_up_to_capacity(self):
        from execution.exchange_adapter import RateLimiter
        rl = RateLimiter(rate_per_second=100, capacity=5)
        for _ in range(5):
            assert rl.acquire(block=False) is True

    def test_blocks_when_empty(self):
        from execution.exchange_adapter import RateLimiter
        rl = RateLimiter(rate_per_second=1, capacity=1)
        rl.acquire(block=False)
        assert rl.acquire(block=False) is False


# ── PaperAdapter ──────────────────────────────────────────────────────────────

class TestPaperAdapter:

    def _make_adapter(self):
        from execution.paper_adapter import PaperAdapter
        mock_pt = MagicMock()
        mock_pt.account_balance = 10000.0
        mock_pt._lock = __import__("threading").RLock()
        mock_pt.open_positions = {}
        mock_pt.execute_signal.return_value = {
            "trade_id": "abc123",
            "position_size": 0.1,
            "entry_price": 2000.0,
        }
        return PaperAdapter(mock_pt), mock_pt

    def test_successful_order(self):
        adapter, mock_pt = self._make_adapter()
        from execution.exchange_adapter import OrderRequest
        req = OrderRequest(symbol="ETH-USD", side="BUY", quantity=0.1, price=2000.0)
        result = adapter.place_order(req)
        assert result.status == "FILLED"
        assert result.order_id == "abc123"

    def test_rejected_order(self):
        adapter, mock_pt = self._make_adapter()
        mock_pt.execute_signal.return_value = None
        from execution.exchange_adapter import OrderRequest
        req = OrderRequest(symbol="ETH-USD", side="BUY", quantity=0.1)
        result = adapter.place_order(req)
        assert result.status == "FAILED"

    def test_get_balance(self):
        adapter, _ = self._make_adapter()
        assert adapter.get_balance() == 10000.0


# ── ExchangeRouter ────────────────────────────────────────────────────────────

class TestExchangeRouter:

    def _make_router(self):
        from execution.exchange_router import ExchangeRouter
        from execution.paper_adapter   import PaperAdapter
        mock_pt = MagicMock()
        mock_pt.account_balance = 10000.0
        mock_pt._lock = __import__("threading").RLock()
        mock_pt.open_positions = {}
        mock_pt.execute_signal.return_value = {
            "trade_id": "r001", "position_size": 1.0, "entry_price": 100.0
        }
        router = ExchangeRouter()
        router.register("paper", PaperAdapter(mock_pt))
        return router

    def test_routes_crypto_to_paper(self):
        router = self._make_router()
        signal = {"asset": "BTC-USD", "category": "crypto",
                  "direction": "BUY", "position_size": 0.001,
                  "entry_price": 50000.0, "confidence": 0.8}
        result = router.submit(signal)
        assert result is not None
        assert result.status == "FILLED"

    def test_returns_none_for_unregistered_category(self):
        from execution.exchange_router import ExchangeRouter
        router = ExchangeRouter()  # no adapters registered
        result = router.submit({"asset": "X", "category": "exotic",
                                "direction": "BUY", "position_size": 1.0})
        assert result is None

    def test_custom_route(self):
        router = self._make_router()
        router.set_route("crypto", "paper")  # explicit — already default
        signal = {"asset": "ETH-USD", "category": "crypto",
                  "direction": "SELL", "position_size": 0.1,
                  "entry_price": 2000.0}
        result = router.submit(signal)
        assert result.status == "FILLED"


# ── PortfolioRiskEngine ───────────────────────────────────────────────────────

class TestPortfolioRiskEngine:

    def _engine(self):
        from risk.portfolio_risk import PortfolioRiskEngine
        return PortfolioRiskEngine(
            max_single_asset_pct=20.0,
            max_category_pct=40.0,
            drawdown_halt_pct=8.0,
            drawdown_reduce_pct=5.0,
        )

    def _signal(self, asset="BTC-USD", category="crypto",
                direction="BUY", size=0.001, price=50000.0):
        return {"asset": asset, "category": category,
                "direction": direction, "position_size": size,
                "entry_price": price, "confidence": 0.8}

    def test_approves_clean_signal(self):
        eng = self._engine()
        ok, reason = eng.evaluate(
            self._signal(), open_positions=[], balance=10000.0,
            initial_balance=10000.0, daily_pnl=0.0
        )
        assert ok is True
        assert reason == ""

    def test_blocks_on_drawdown_halt(self):
        eng = self._engine()
        eng._peak_balance = 10000.0
        ok, reason = eng.evaluate(
            self._signal(), open_positions=[], balance=9100.0,
            initial_balance=10000.0, daily_pnl=-900.0
        )
        assert ok is False
        assert "drawdown" in reason.lower()

    def test_blocks_on_asset_overexposure(self):
        eng = self._engine()
        eng._peak_balance = 10000.0
        # Already have $2500 in BTC (25% of 10000)
        existing = [{"asset": "BTC-USD", "category": "crypto",
                     "direction": "BUY", "position_size": 0.05,
                     "entry_price": 50000.0}]
        ok, reason = eng.evaluate(
            self._signal(size=0.001, price=50000.0),
            open_positions=existing, balance=10000.0,
            initial_balance=10000.0, daily_pnl=0.0
        )
        assert ok is False
        assert "exposure" in reason.lower()

    def test_blocks_correlation_risk(self):
        eng = self._engine()
        eng._peak_balance = 10000.0
        # 3 existing BUY positions in crypto
        existing = [
            {"asset": f"COIN{i}-USD", "category": "crypto",
             "direction": "BUY", "position_size": 0.001,
             "entry_price": 100.0}
            for i in range(3)
        ]
        ok, reason = eng.evaluate(
            self._signal(asset="NEW-USD", size=0.001, price=100.0),
            open_positions=existing, balance=10000.0,
            initial_balance=10000.0, daily_pnl=0.0
        )
        assert ok is False
        assert "correlation" in reason.lower()

    def test_get_portfolio_stats_empty(self):
        eng = self._engine()
        stats = eng.get_portfolio_stats([], 10000.0)
        assert stats["total_exposure"] == 0.0
        assert stats["position_count"] == 0

    def test_scales_position_on_partial_drawdown(self):
        eng = self._engine()
        eng._peak_balance = 10000.0
        sig = self._signal(size=0.001, price=50000.0)
        # 6% drawdown — between reduce (5%) and halt (8%)
        ok, _ = eng.evaluate(
            sig, open_positions=[], balance=9400.0,
            initial_balance=10000.0, daily_pnl=-600.0
        )
        assert ok is True
        assert sig["position_size"] < 0.001  # scaled down


# ── PredictionClient fallback ─────────────────────────────────────────────────

class TestPredictionClient:

    def test_falls_back_when_service_unreachable(self):
        from ml.prediction_service import PredictionClient
        import pandas as pd
        import numpy as np

        client = PredictionClient(host="127.0.0.1", port=19876)  # nothing there
        df = pd.DataFrame({
            "open":   np.random.uniform(100, 110, 50),
            "high":   np.random.uniform(110, 120, 50),
            "low":    np.random.uniform(90,  100, 50),
            "close":  np.random.uniform(100, 110, 50),
            "volume": np.random.uniform(1000, 2000, 50),
        })
        direction, prob = client.predict_next(df, "crypto", "BTC-USD")
        assert direction in ("BUY", "SELL", "HOLD")
        assert 0.0 <= prob <= 1.0