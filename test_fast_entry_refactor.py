from datetime import datetime, timezone
import importlib


def _breakout_candidate() -> dict:
    return {
        "playbook": "breakout_continuation",
        "direction": "BUY",
        "score": 0.74,
        "confidence": 0.76,
        "entry_style": "breakout_close",
        "preferred_interval": "5m",
    }


def _pullback_candidate() -> dict:
    return {
        "playbook": "trend_pullback",
        "direction": "BUY",
        "score": 0.71,
        "confidence": 0.73,
        "entry_style": "pullback_hold",
        "preferred_interval": "5m",
    }


def test_fast_breakout_continuation_allows_5m_trigger_relief(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    candidate = _breakout_candidate()
    structure = {
        "structure_bias": "buy",
        "alignment_score": 0.64,
        "setup_quality": 0.63,
        "upside_exhaustion_score": 0.12,
        "downside_exhaustion_score": 0.0,
        "trend_5m": "trending_up",
        "trend_15m": "trending_up",
        "trend_1h": "ranging",
        "pattern_family": "trending_up_generic",
        "entry_confirmation_ready": False,
        "entry_confirmation_count": 0,
        "entry_confirmation_bars_required": 2,
        "fast_entry_confirmation_ready": True,
        "fast_entry_confirmation_count": 1,
        "fast_entry_confirmation_bars_required": 1,
        "trigger_trend_aligned": True,
        "structure_promoted": True,
        "external_confirmation_score": 0.18,
        "liquidity_sweep_buy": False,
        "liquidity_sweep_sell": False,
    }

    approved, reason = service._qualify_candidate(
        candidate,
        asset="EUR/USD",
        category="forex",
        structure=structure,
        plan=service._asset_plan("EUR/USD", "forex"),
    )

    assert approved is True
    assert reason == ""
    assert candidate["qualification"]["effective_required_trends"] == 1
    assert candidate["qualification"]["allow_early_trend_relief"] is True
    assert candidate["qualification"]["fast_confirmation_override"] is True


def test_premium_pullback_keeps_dual_trend_requirement(monkeypatch) -> None:
    svc_mod = importlib.import_module("services.playbook_service")
    monkeypatch.setattr(
        svc_mod,
        "_utc_now",
        lambda: datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc),
    )
    service = svc_mod.get_service()
    candidate = _pullback_candidate()
    structure = {
        "structure_bias": "buy",
        "alignment_score": 0.70,
        "setup_quality": 0.69,
        "upside_exhaustion_score": 0.14,
        "downside_exhaustion_score": 0.0,
        "trend_5m": "trending_up",
        "trend_15m": "trending_up",
        "trend_1h": "ranging",
        "pattern_family": "trending_up_first_pullback",
        "first_pullback_ready": True,
        "entry_confirmation_ready": False,
        "entry_confirmation_count": 0,
        "entry_confirmation_bars_required": 2,
        "fast_entry_confirmation_ready": True,
        "fast_entry_confirmation_count": 1,
        "fast_entry_confirmation_bars_required": 1,
        "trigger_trend_aligned": True,
        "structure_promoted": True,
        "external_confirmation_score": 0.22,
        "liquidity_sweep_buy": False,
        "liquidity_sweep_sell": False,
    }

    approved, reason = service._qualify_candidate(
        candidate,
        asset="EUR/USD",
        category="forex",
        structure=structure,
        plan=service._asset_plan("EUR/USD", "forex"),
    )

    assert approved is False
    assert reason == "trend_misaligned:trend_pullback"
    assert candidate["qualification"]["effective_required_trends"] == 2


def test_structure_intervals_for_15m_include_5m_trigger_frame() -> None:
    engine_mod = importlib.import_module("core.engine")
    intervals = engine_mod.TradingCore._get_structure_intervals("15m")

    assert intervals[:2] == ["5m", "1h"]
    assert "4h" in intervals
