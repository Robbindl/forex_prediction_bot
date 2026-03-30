from __future__ import annotations

from typing import Any, Dict

from config.config import (
    GOVERNANCE_ENABLE_FOREX_FILTER,
    GOVERNANCE_MIN_LIVE_ACCURACY,
    GOVERNANCE_MIN_LIVE_SAMPLES,
    GOVERNANCE_MIN_ML_CONFIDENCE,
    GOVERNANCE_MIN_REAL_SOURCES,
    GOVERNANCE_MIN_RISK_REWARD,
    GOVERNANCE_REQUIRE_MODEL_RESEARCH,
    GOVERNANCE_REQUIRE_NON_DELAYED_OHLCV,
    GOVERNANCE_REQUIRE_NON_DELAYED_PRICE,
    GOVERNANCE_VALIDATION_DAYS,
    GOVERNANCE_VALIDATION_HORIZON,
)
from ml.registry import registry
from risk.forex_filter import ForexFilter
from utils.logger import get_logger

logger = get_logger()

MODE_NAME = "deriv"

_ACCEPTABLE_SOURCE_CLASSES = {"stream", "broker", "primary_api", "secondary_api", "cache"}
_SOURCE_SCORE = {
    "stream": 96,
    "broker": 92,
    "primary_api": 88,
    "secondary_api": 76,
    "cache": 70,
    "fallback": 40,
    "unknown": 10,
    "unavailable": 0,
}


class SignalGovernance:
    def evaluate(self, signal, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        context = context or {}
        asset = signal.canonical_asset or signal.asset
        model_key = (
            signal.metadata.get("policy_model")
            or signal.metadata.get("seed_model")
            or f"{signal.category}_classifier"
        )
        model_meta = registry.get_metadata(model_key) if model_key else {}
        market_data = context.get("market_data") or signal.metadata.get("market_data") or {}
        price_meta = market_data.get("price") or {}
        ohlcv_meta = market_data.get("ohlcv") or {}
        live_validation = self._get_live_validation(asset)

        violations = []
        warnings = []

        valid_sources = int(signal.metadata.get("valid_sources_count", 0) or 0)
        market_intel_score = float(signal.metadata.get("market_intelligence_score", 0.0) or 0.0)
        market_intel_sources = list(signal.metadata.get("market_intelligence_sources") or [])
        ml_conf = float(signal.metadata.get("ml_confidence", context.get("ml_confidence", 0.0)) or 0.0)
        live_total = int(live_validation.get("total", 0) or 0)
        live_accuracy = float(live_validation.get("accuracy_pct", 0.0) or 0.0)
        price_score = self._source_score(price_meta)
        ohlcv_score = self._source_score(ohlcv_meta)
        data_quality_score = round(price_score * 0.6 + ohlcv_score * 0.4, 1)
        delayed_data = bool(price_meta.get("delayed", False)) or bool(ohlcv_meta.get("delayed", False))
        research_ok, research_note = self._assess_model_research(model_key, model_meta)

        if valid_sources < GOVERNANCE_MIN_REAL_SOURCES:
            violations.append(f"real_sources={valid_sources} below minimum {GOVERNANCE_MIN_REAL_SOURCES}")

        if ml_conf < GOVERNANCE_MIN_ML_CONFIDENCE:
            violations.append(
                f"ml_confidence={ml_conf:.3f} below minimum {GOVERNANCE_MIN_ML_CONFIDENCE:.3f}"
            )

        if float(signal.risk_reward or 0.0) < GOVERNANCE_MIN_RISK_REWARD:
            violations.append(
                f"risk_reward={float(signal.risk_reward or 0.0):.2f} below minimum {GOVERNANCE_MIN_RISK_REWARD:.2f}"
            )

        if GOVERNANCE_REQUIRE_MODEL_RESEARCH and not research_ok:
            violations.append(research_note)
        elif research_note:
            warnings.append(research_note)

        price_ok, price_reason = self._check_market_source(
            price_meta,
            require_non_delayed=GOVERNANCE_REQUIRE_NON_DELAYED_PRICE,
            label="price",
        )
        if not price_ok:
            violations.append(price_reason)

        ohlcv_ok, ohlcv_reason = self._check_market_source(
            ohlcv_meta,
            require_non_delayed=GOVERNANCE_REQUIRE_NON_DELAYED_OHLCV,
            label="ohlcv",
        )
        if not ohlcv_ok:
            violations.append(ohlcv_reason)

        if signal.category == "crypto":
            crypto_price_ok, crypto_price_note = self._check_crypto_price_source(price_meta)
            if not crypto_price_ok:
                violations.append(crypto_price_note)
            elif crypto_price_note:
                warnings.append(crypto_price_note)

        live_violation, live_warning = self._assess_live_validation(live_validation)
        if live_violation:
            violations.append(live_violation)
        elif live_warning:
            warnings.append(live_warning)

        if signal.category == "forex" and GOVERNANCE_ENABLE_FOREX_FILTER:
            passed, reason = self._run_forex_filter(signal, context)
            if not passed:
                violations.append(f"forex quality: {reason}")

        if price_meta.get("from_cache"):
            warnings.append(f"price source cached via {price_meta.get('source', 'unknown')}")
        if ohlcv_meta.get("from_cache"):
            warnings.append(f"ohlcv source cached via {ohlcv_meta.get('source', 'unknown')}")

        score = data_quality_score
        if bool(model_meta.get("research_approved")):
            score += 10
        elif research_ok:
            score += 6
        elif float(model_meta.get("holdout_accuracy", 0.0) or 0.0) >= 0.52:
            score += 4
        if live_total >= max(1, GOVERNANCE_MIN_LIVE_SAMPLES):
            score += max(-12, min(12, live_accuracy - 50.0))
        elif live_total > 0:
            score += 2
        score += min(8, valid_sources * 2)
        score += min(10, ml_conf * 20)
        if market_intel_sources:
            score += min(6, max(1, len(market_intel_sources)))
            score += min(5, abs(market_intel_score) * 5)
        if delayed_data:
            score -= 10
        score -= min(16, len(warnings) * 2)
        score -= min(30, len(violations) * 6)
        score = max(0, min(100, round(score)))
        grade = "A" if score >= 90 else "B" if score >= 80 else "C" if score >= 70 else "D"

        approved = not violations
        reason = "deriv governance passed" if approved else "; ".join(violations) or f"score {score} below floor"
        return {
            "approved": approved,
            "reason": reason,
            "mode": MODE_NAME,
            "grade": grade,
            "score": score,
            "data_quality_score": data_quality_score,
            "violations": violations,
            "warnings": warnings,
            "model_key": model_key,
            "model_research": model_meta,
            "live_validation": live_validation,
            "market_data": {
                "price": price_meta,
                "ohlcv": ohlcv_meta,
            },
        }

    @staticmethod
    def _assess_model_research(model_key: str, model_meta: Dict[str, Any]) -> tuple[bool, str]:
        if bool(model_meta.get("research_approved")):
            return True, ""

        holdout_accuracy = float(model_meta.get("holdout_accuracy", 0.0) or 0.0)
        holdout_threshold = float(model_meta.get("holdout_threshold", 0.52) or 0.52)
        walk_forward_accuracy = float(model_meta.get("walk_forward_accuracy", 0.0) or 0.0)
        walk_forward_threshold = float(model_meta.get("walk_forward_threshold", 0.52) or 0.52)
        walk_forward_samples = int(model_meta.get("walk_forward_samples", 0) or 0)
        walk_forward_required = int(model_meta.get("walk_forward_required_samples", 60) or 60)
        research_state = str(
            model_meta.get("research_status")
            or model_meta.get("research_grade")
            or ""
        ).strip().lower()

        provisional_ok = (
            research_state == "provisional"
            and holdout_accuracy >= holdout_threshold
            and walk_forward_accuracy >= walk_forward_threshold
            and walk_forward_samples >= walk_forward_required
        )
        if provisional_ok:
            return True, f"model {model_key or 'unknown'} using bootstrap research allowance ({research_state})"

        return False, f"model {model_key or 'unknown'} lacks approved walk-forward research"

    @staticmethod
    def _check_market_source(
        meta: Dict[str, Any],
        require_non_delayed: bool,
        label: str,
    ) -> tuple[bool, str]:
        if not meta:
            return False, f"{label} provenance missing"
        source = str(meta.get("source", "unknown"))
        source_class = str(meta.get("source_class", "unknown"))
        delayed = bool(meta.get("delayed", False))

        if require_non_delayed and delayed:
            return False, f"{label} source {source} is delayed"
        if source_class not in _ACCEPTABLE_SOURCE_CLASSES:
            return False, f"{label} source {source} classified as {source_class}"
        return True, ""

    @staticmethod
    def _check_crypto_price_source(meta: Dict[str, Any]) -> tuple[bool, str]:
        if not meta:
            return False, "crypto price provenance missing"

        source = str(meta.get("source", "unknown"))
        source_class = str(meta.get("source_class", "unknown"))
        delayed = bool(meta.get("delayed", False))
        realtime = bool(meta.get("realtime", False))

        if delayed:
            return False, "crypto requires fresh non-delayed price data"
        if source_class in {"stream", "broker", "primary_api"}:
            return True, ""
        if source_class == "secondary_api" and realtime:
            return True, f"crypto using realtime secondary price source {source}"
        return False, "crypto requires fresh stream, primary, or realtime secondary price data"

    @staticmethod
    def _source_score(meta: Dict[str, Any]) -> int:
        if not meta:
            return 0
        source_class = str(meta.get("source_class", "unknown"))
        score = _SOURCE_SCORE.get(source_class, 0)
        if bool(meta.get("delayed", False)):
            score = max(0, score - 20)
        if bool(meta.get("from_cache", False)):
            score = max(0, score - 6)
        return score

    @staticmethod
    def _assess_live_validation(live_validation: Dict[str, Any]) -> tuple[str, str]:
        live_total = int(live_validation.get("total", 0) or 0)
        live_accuracy = float(live_validation.get("accuracy_pct", 0.0) or 0.0)
        scope = live_validation.get("scope", "fallback")

        if live_total <= 0:
            return "", "live validation bootstrap: no evaluated samples yet"
        if live_total < GOVERNANCE_MIN_LIVE_SAMPLES:
            return "", (
                f"live validation bootstrap: {live_total}/{GOVERNANCE_MIN_LIVE_SAMPLES} "
                f"evaluated samples"
            )
        if live_accuracy < GOVERNANCE_MIN_LIVE_ACCURACY:
            return (
                f"live {scope} accuracy {live_accuracy:.1f}% "
                f"below minimum {GOVERNANCE_MIN_LIVE_ACCURACY:.1f}%",
                "",
            )
        return "", ""

    @staticmethod
    def _run_forex_filter(signal, context: Dict[str, Any]) -> tuple[bool, str]:
        df = context.get("price_data")
        if df is None or len(df) < 30:
            return False, "insufficient price history"

        try:
            high = df["high"].astype(float)
            low = df["low"].astype(float)
            close = df["close"].astype(float)
            prev_close = close.shift(1)
            tr = (
                (high - low).to_frame("hl")
                .join((high - prev_close).abs().to_frame("hc"))
                .join((low - prev_close).abs().to_frame("lc"))
                .max(axis=1)
            )
            atr = float(tr.tail(14).mean())
        except Exception:
            return False, "ATR unavailable"

        spread = context.get("spread")
        spread_bps = None
        try:
            if spread and signal.entry_price:
                spread_bps = float(spread) / float(signal.entry_price) * 10000
        except Exception:
            spread_bps = None

        return ForexFilter.should_trade_forex_signal(
            asset=signal.asset,
            signal_confidence=float(signal.confidence),
            df=df,
            atr=atr,
            current_spread_bps=spread_bps,
        )

    @staticmethod
    def _get_live_validation(asset: str) -> Dict[str, Any]:
        try:
            from prediction_tracker import prediction_tracker as tracker

            stats = tracker.get_accuracy_stats(days_back=GOVERNANCE_VALIDATION_DAYS)
        except Exception as exc:
            logger.debug(f"[SignalGovernance] Prediction tracker unavailable: {exc}")
            return {"scope": "unavailable", "total": 0, "accuracy_pct": 0.0}

        label = GOVERNANCE_VALIDATION_HORIZON
        by_asset = (stats.get("by_asset") or {}).get(asset, {})
        asset_stats = by_asset.get(label)
        if asset_stats and int(asset_stats.get("total", 0) or 0) > 0:
            return {"scope": "asset", **asset_stats}

        by_horizon = stats.get("by_horizon") or {}
        global_stats = by_horizon.get(label, {})
        return {"scope": "portfolio", **global_stats}


signal_governance = SignalGovernance()
