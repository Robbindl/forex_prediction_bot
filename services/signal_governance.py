from __future__ import annotations

import os
from typing import Any, Dict

from config.config import (
    GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY,
    GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES,
    GOVERNANCE_ALLOW_PROVISIONAL_MODEL_RESEARCH_IN_PAPER,
    GOVERNANCE_ENABLE_FOREX_FILTER,
    GOVERNANCE_EXPECTANCY_MAX_PREMATURE_STOP_RATE,
    GOVERNANCE_EXPECTANCY_MIN_AVG_R,
    GOVERNANCE_EXPECTANCY_MIN_QUALITY_SCORE,
    GOVERNANCE_EXPECTANCY_MIN_SAMPLES,
    GOVERNANCE_EXPECTANCY_MIN_TARGET_HIT_RATE,
    GOVERNANCE_MIN_LIVE_ACCURACY,
    GOVERNANCE_MIN_LIVE_SAMPLES,
    GOVERNANCE_MIN_ML_CONFIDENCE,
    GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY,
    GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES,
    GOVERNANCE_MIN_REAL_SOURCES,
    GOVERNANCE_MIN_RISK_REWARD,
    GOVERNANCE_REQUIRE_MODEL_RESEARCH,
    GOVERNANCE_REQUIRE_NON_DELAYED_OHLCV,
    GOVERNANCE_REQUIRE_NON_DELAYED_PRICE,
    GOVERNANCE_VALIDATION_DAYS,
    GOVERNANCE_VALIDATION_HORIZON,
    PLAYBOOK_ONLY_RUNTIME,
)
from risk.forex_filter import ForexFilter
from utils.logger import get_logger

logger = get_logger()

MODE_NAME = "deriv"

_ACCEPTABLE_SOURCE_CLASSES = {"stream", "broker", "primary_api", "secondary_api", "cache", "local_store"}
_SOURCE_SCORE = {
    "stream": 96,
    "broker": 92,
    "primary_api": 88,
    "local_store": 84,
    "secondary_api": 76,
    "cache": 70,
    "fallback": 40,
    "unknown": 10,
    "unavailable": 0,
}

_LIVE_CATEGORY_GOVERNANCE_PROFILES: Dict[str, Dict[str, float | bool]] = {
    "crypto": {
        "min_risk_reward": 1.35,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 45.0,
        "bootstrap_min_live_accuracy": 42.0,
        "expectancy_min_avg_r": -0.05,
    },
    "commodities": {
        "min_risk_reward": 1.15,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 50.0,
        "bootstrap_min_live_accuracy": 45.0,
        "expectancy_min_avg_r": -0.02,
        "allow_provisional_research_in_paper": True,
    },
    "forex": {
        "min_risk_reward": 1.15,
    },
    "indices": {
        "min_risk_reward": 1.25,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 50.0,
        "bootstrap_min_live_accuracy": 45.0,
    },
}

_PAPER_CATEGORY_GOVERNANCE_PROFILES: Dict[str, Dict[str, float | bool]] = {
    "crypto": {
        "min_risk_reward": 1.20,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 25.0,
        "bootstrap_min_live_accuracy": 22.0,
        "expectancy_min_avg_r": -0.05,
    },
    "commodities": {
        "min_risk_reward": 1.00,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 50.0,
        "bootstrap_min_live_accuracy": 45.0,
        "expectancy_min_avg_r": -0.02,
        "allow_provisional_research_in_paper": True,
    },
    "forex": {
        "min_risk_reward": 0.75,
    },
    "indices": {
        "min_risk_reward": 1.00,
        "min_ml_confidence": 0.14,
        "min_live_accuracy": 45.0,
        "bootstrap_min_live_accuracy": 40.0,
    },
}


class SignalGovernance:
    def evaluate(self, signal, context: Dict[str, Any] | None = None) -> Dict[str, Any]:
        context = context or {}
        asset = signal.canonical_asset or signal.asset
        category = str(signal.category or "").strip().lower()
        model_key = str(
            signal.metadata.get("playbook_name")
            or signal.metadata.get("seed_model")
            or "playbook_runtime"
        ).strip()
        research_model_key, model_meta, research_warning = self._resolve_research_model(signal, model_key)
        market_data = context.get("market_data") or signal.metadata.get("market_data") or {}
        price_meta = market_data.get("price") or {}
        ohlcv_meta = market_data.get("ohlcv") or {}
        live_validation = self._get_live_validation(asset)
        registry_validation = self._get_registry_validation(asset, signal.category)
        expectancy_validation = self._get_expectancy_validation(asset, signal.category)
        adaptive_policy = context.get("adaptive_policy") or signal.metadata.get("adaptive_policy") or {}
        min_risk_reward = self._effective_min_risk_reward(adaptive_policy, category=category)
        min_ml_confidence = self._effective_min_ml_confidence(category)

        violations = []
        warnings = []
        if research_warning:
            warnings.append(research_warning)

        valid_sources = int(signal.metadata.get("valid_sources_count", 0) or 0)
        market_intel_score = float(signal.metadata.get("market_intelligence_score", 0.0) or 0.0)
        market_intel_sources = list(signal.metadata.get("market_intelligence_sources") or [])
        ml_conf = float(signal.metadata.get("ml_confidence", context.get("ml_confidence", 0.0)) or 0.0)
        seed_source = str(signal.metadata.get("seed_source", "") or "").strip().lower()
        playbook_action = str(signal.metadata.get("playbook_action", "") or "").strip().lower()
        playbook_confidence = float(signal.metadata.get("playbook_confidence", 0.0) or 0.0)
        effective_seed_confidence = ml_conf
        if seed_source == "playbook" or playbook_action in {"seed", "override"}:
            effective_seed_confidence = max(effective_seed_confidence, playbook_confidence)
        live_total = int(live_validation.get("total", 0) or 0)
        live_accuracy = float(live_validation.get("accuracy_pct", 0.0) or 0.0)
        price_score = self._source_score(price_meta)
        ohlcv_score = self._source_score(ohlcv_meta)
        data_quality_score = round(price_score * 0.6 + ohlcv_score * 0.4, 1)
        delayed_data = bool(price_meta.get("delayed", False)) or bool(ohlcv_meta.get("delayed", False))
        research_ok, research_note = self._assess_model_research(
            research_model_key,
            model_meta,
            category=category,
        )

        if valid_sources < GOVERNANCE_MIN_REAL_SOURCES:
            violations.append(f"real_sources={valid_sources} below minimum {GOVERNANCE_MIN_REAL_SOURCES}")

        if effective_seed_confidence < min_ml_confidence:
            if seed_source == "playbook" or playbook_action in {"seed", "override"}:
                violations.append(
                    f"seed_confidence={effective_seed_confidence:.3f} below minimum {min_ml_confidence:.3f} "
                    f"(ml={ml_conf:.3f}, playbook={playbook_confidence:.3f})"
                )
            else:
                violations.append(
                    f"ml_confidence={ml_conf:.3f} below minimum {min_ml_confidence:.3f}"
                )

        if float(signal.risk_reward or 0.0) < min_risk_reward:
            violations.append(
                f"risk_reward={float(signal.risk_reward or 0.0):.2f} below minimum {min_risk_reward:.2f}"
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

        registry_violation, registry_warning = self._assess_registry_validation(registry_validation)
        if registry_violation:
            violations.append(registry_violation)
        elif registry_warning:
            warnings.append(registry_warning)

        live_violation, live_warning = self._assess_live_validation(live_validation, category=category)
        if live_violation:
            violations.append(live_violation)
        elif live_warning:
            warnings.append(live_warning)

        expectancy_violation, expectancy_warning = self._assess_expectancy_validation(
            expectancy_validation,
            category=category,
        )
        if expectancy_violation:
            violations.append(expectancy_violation)
        elif expectancy_warning:
            warnings.append(expectancy_warning)

        if signal.category == "forex" and GOVERNANCE_ENABLE_FOREX_FILTER:
            passed, reason = self._run_forex_filter(signal, {**context, "live_validation": live_validation})
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
        if registry_validation.get("exact_match"):
            score += 12
        elif registry_validation.get("matched"):
            score += 6
        if live_total >= max(1, GOVERNANCE_MIN_LIVE_SAMPLES):
            score += max(-12, min(12, live_accuracy - 50.0))
        elif live_total > 0:
            score += 2
        expectancy_samples = int(expectancy_validation.get("sample_count", 0) or 0)
        expectancy_avg_r = float(expectancy_validation.get("avg_rr_realized", 0.0) or 0.0)
        if expectancy_samples >= max(1, GOVERNANCE_EXPECTANCY_MIN_SAMPLES):
            score += max(-10, min(10, expectancy_avg_r * 14.0))
        elif expectancy_samples > 0:
            score += 2
        score += min(8, valid_sources * 2)
        score += min(10, effective_seed_confidence * 20)
        if seed_source == "playbook" and playbook_action in {"seed", "override"}:
            score += min(4, max(0.0, playbook_confidence - 0.5) * 10)
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
            "research_model_key": research_model_key,
            "model_research": model_meta,
            "live_validation": live_validation,
            "registry_validation": registry_validation,
            "expectancy_validation": expectancy_validation,
            "market_data": {
                "price": price_meta,
                "ohlcv": ohlcv_meta,
            },
            "min_risk_reward": round(min_risk_reward, 2),
            "effective_seed_confidence": round(effective_seed_confidence, 4),
        }

    @staticmethod
    def _category_profile(category: str) -> Dict[str, float | bool]:
        base: Dict[str, float | bool] = {
            "min_risk_reward": float(GOVERNANCE_MIN_RISK_REWARD),
            "min_ml_confidence": float(GOVERNANCE_MIN_ML_CONFIDENCE),
            "min_live_accuracy": float(GOVERNANCE_MIN_LIVE_ACCURACY),
            "bootstrap_min_live_accuracy": float(GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY),
            "expectancy_min_avg_r": float(GOVERNANCE_EXPECTANCY_MIN_AVG_R),
            "allow_provisional_research_in_paper": False,
        }
        category_key = str(category or "").strip().lower()
        runtime_live = os.getenv("BOT_LIVE_RUNTIME", "0") == "1"
        profile_source = (
            _LIVE_CATEGORY_GOVERNANCE_PROFILES
            if runtime_live
            else _PAPER_CATEGORY_GOVERNANCE_PROFILES
        )
        base.update(profile_source.get(category_key, {}))
        return base

    @classmethod
    def _effective_min_ml_confidence(cls, category: str) -> float:
        profile = cls._category_profile(category)
        return max(0.05, float(profile.get("min_ml_confidence", GOVERNANCE_MIN_ML_CONFIDENCE) or GOVERNANCE_MIN_ML_CONFIDENCE))

    @classmethod
    def _effective_min_risk_reward(cls, adaptive_policy: Dict[str, Any], *, category: str = "") -> float:
        runtime_live = os.getenv("BOT_LIVE_RUNTIME", "0") == "1"
        category_key = str(category or "").strip().lower()
        if runtime_live:
            rr_floor = 1.0
        elif category_key == "forex":
            rr_floor = 0.65
        elif category_key in {"commodities", "indices"}:
            rr_floor = 0.85
        elif category_key == "crypto":
            rr_floor = 1.0
        else:
            rr_floor = 0.75
        try:
            adaptive_min_rr = float((adaptive_policy or {}).get("min_rr", 0.0) or 0.0)
        except Exception:
            adaptive_min_rr = 0.0
        if adaptive_min_rr > 0.0:
            return max(rr_floor, adaptive_min_rr)
        profile = cls._category_profile(category)
        return max(
            rr_floor,
            float(profile.get("min_risk_reward", GOVERNANCE_MIN_RISK_REWARD) or GOVERNANCE_MIN_RISK_REWARD),
        )

    @staticmethod
    def _has_research_metadata(model_meta: Dict[str, Any]) -> bool:
        if not model_meta:
            return False
        return any(
            key in model_meta
            for key in (
                "research_approved",
                "research_status",
                "research_grade",
                "holdout_accuracy",
                "walk_forward_accuracy",
            )
        )

    @classmethod
    def _resolve_research_model(cls, signal, decision_model_key: str) -> tuple[str, Dict[str, Any], str]:
        decision_model_key = str(decision_model_key or "").strip() or "playbook_runtime"
        decision_meta = dict(signal.metadata.get("playbook_research") or {})
        if not cls._has_research_metadata(decision_meta):
            decision_meta = {
                "research_approved": True,
                "research_status": "playbook_runtime",
                "research_grade": "runtime",
            }
        return decision_model_key, decision_meta, ""

    @classmethod
    def _assess_model_research(
        cls,
        model_key: str,
        model_meta: Dict[str, Any],
        *,
        category: str = "",
    ) -> tuple[bool, str]:
        research_state = str(
            model_meta.get("research_status")
            or model_meta.get("research_grade")
            or ""
        ).strip().lower()
        if research_state in {"playbook_runtime", "runtime"}:
            return True, ""
        if bool(model_meta.get("research_approved")):
            return True, ""

        holdout_accuracy = float(model_meta.get("holdout_accuracy", 0.0) or 0.0)
        holdout_threshold = float(model_meta.get("holdout_threshold", 0.52) or 0.52)
        walk_forward_accuracy = float(model_meta.get("walk_forward_accuracy", 0.0) or 0.0)
        walk_forward_threshold = float(model_meta.get("walk_forward_threshold", 0.52) or 0.52)
        walk_forward_samples = int(model_meta.get("walk_forward_samples", 0) or 0)
        walk_forward_required = int(model_meta.get("walk_forward_required_samples", 60) or 60)
        category_name = str(category or "").strip().lower()
        runtime_live = os.getenv("BOT_LIVE_RUNTIME", "0") == "1"

        provisional_ok = (
            research_state == "provisional"
            and holdout_accuracy >= holdout_threshold
            and walk_forward_accuracy >= walk_forward_threshold
            and walk_forward_samples >= walk_forward_required
        )
        profile = cls._category_profile(category_name)
        allow_paper_provisional = bool(profile.get("allow_provisional_research_in_paper", False))

        if category_name == "commodities":
            if provisional_ok and not runtime_live and allow_paper_provisional:
                return True, (
                    f"model {model_key or 'unknown'} allowed in paper runtime with provisional commodity research"
                )
            return False, f"model {model_key or 'unknown'} lacks approved walk-forward research"

        if provisional_ok:
            return True, f"model {model_key or 'unknown'} using bootstrap research allowance ({research_state})"

        paper_provisional_ok = (
            not runtime_live
            and GOVERNANCE_ALLOW_PROVISIONAL_MODEL_RESEARCH_IN_PAPER
            and research_state == "provisional"
        )
        if paper_provisional_ok:
            return True, (
                f"model {model_key or 'unknown'} allowed in paper runtime with provisional research"
            )

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
    def _assess_registry_validation(registry_validation: Dict[str, Any]) -> tuple[str, str]:
        required = bool(registry_validation.get("required"))
        asset_required = bool(registry_validation.get("asset_required"))
        bootstrap_mode = bool(registry_validation.get("bootstrap_mode"))
        asset = str(registry_validation.get("asset") or "")
        category = str(registry_validation.get("category") or "")
        matched = bool(registry_validation.get("matched"))
        exact_match = bool(registry_validation.get("exact_match"))
        match_scope = str(registry_validation.get("match_scope") or "none")
        names = list(registry_validation.get("names") or [])

        if bootstrap_mode:
            return "", (
                "live strategy registry empty during live runtime — "
                "bootstrap mode active until at least one strategy is promoted"
            )

        if not required:
            if matched and match_scope != "asset":
                return "", f"using {match_scope} live strategy approval for {asset or category}"
            return "", ""

        if not matched:
            return f"no approved live strategy for {asset} ({category}) in live registry", ""
        if asset_required and not exact_match:
            return (
                f"no asset-specific approved live strategy for {asset} ({category}) — only {match_scope} registry matches found",
                "",
            )
        if match_scope != "asset":
            joined = ", ".join([name for name in names if name][:3])
            note = f"using {match_scope} live strategy approval"
            if joined:
                note = f"{note}: {joined}"
            return "", note
        return "", ""

    @classmethod
    def _assess_live_validation(cls, live_validation: Dict[str, Any], *, category: str = "") -> tuple[str, str]:
        live_total = int(live_validation.get("total", 0) or 0)
        live_accuracy = float(live_validation.get("accuracy_pct", 0.0) or 0.0)
        scope = live_validation.get("scope", "fallback")
        profile = cls._category_profile(category)
        min_live_accuracy = float(profile.get("min_live_accuracy", GOVERNANCE_MIN_LIVE_ACCURACY) or GOVERNANCE_MIN_LIVE_ACCURACY)
        bootstrap_min_live_accuracy = float(
            profile.get("bootstrap_min_live_accuracy", GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY)
            or GOVERNANCE_BOOTSTRAP_MIN_LIVE_ACCURACY
        )

        if scope == "unavailable":
            return "", "live validation unavailable"
        if scope == "bootstrap":
            return "", "live validation bootstrap: no evaluated samples yet"
        if scope == "portfolio_context":
            portfolio_total = int(live_validation.get("portfolio_total", 0) or 0)
            portfolio_accuracy = float(live_validation.get("portfolio_accuracy_pct", 0.0) or 0.0)
            if portfolio_total <= 0:
                return "", "live validation bootstrap: no evaluated samples yet"
            if portfolio_total < GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES:
                return "", (
                    f"asset live validation bootstrap: no asset samples yet; "
                    f"portfolio context {portfolio_total}/{GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES} evaluated samples"
                )
            if portfolio_accuracy < GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY:
                return "", (
                    f"asset live validation bootstrap: no asset samples yet; "
                    f"portfolio context weak at {portfolio_accuracy:.1f}%"
                )
            return "", (
                f"asset live validation bootstrap: no asset samples yet; "
                f"portfolio context {portfolio_accuracy:.1f}% across {portfolio_total} samples"
            )
        if live_total <= 0:
            return "", "live validation bootstrap: no evaluated samples yet"
        if scope == "portfolio":
            if live_total < GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES:
                return "", (
                    f"live portfolio bootstrap: {live_total}/{GOVERNANCE_PORTFOLIO_MIN_LIVE_SAMPLES} "
                    f"evaluated samples"
                )
            if live_accuracy < GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY:
                return (
                    f"live portfolio accuracy {live_accuracy:.1f}% "
                    f"below minimum {GOVERNANCE_PORTFOLIO_MIN_LIVE_ACCURACY:.1f}%",
                    "",
                )
            if live_accuracy < min_live_accuracy:
                return "", (
                    f"live portfolio accuracy {live_accuracy:.1f}% "
                    f"below preferred {min_live_accuracy:.1f}%"
                )
            return "", ""
        if live_total < GOVERNANCE_MIN_LIVE_SAMPLES:
            if (
                live_total >= GOVERNANCE_BOOTSTRAP_MIN_LIVE_SAMPLES
                and live_accuracy < bootstrap_min_live_accuracy
            ):
                return (
                    f"live {scope} bootstrap accuracy {live_accuracy:.1f}% "
                    f"below minimum {bootstrap_min_live_accuracy:.1f}% "
                    f"after {live_total} samples",
                    "",
                )
            return "", (
                f"live validation bootstrap: {live_total}/{GOVERNANCE_MIN_LIVE_SAMPLES} "
                f"evaluated samples"
            )
        if live_accuracy < min_live_accuracy:
            return (
                f"live {scope} accuracy {live_accuracy:.1f}% "
                f"below minimum {min_live_accuracy:.1f}%",
                "",
            )
        return "", ""

    @classmethod
    def _assess_expectancy_validation(cls, expectancy_validation: Dict[str, Any], *, category: str = "") -> tuple[str, str]:
        scope = str(expectancy_validation.get("scope") or "bootstrap")
        sample_count = int(expectancy_validation.get("sample_count", 0) or 0)
        avg_rr_realized = float(expectancy_validation.get("avg_rr_realized", 0.0) or 0.0)
        target_hit_rate = float(expectancy_validation.get("target_hit_rate", 0.0) or 0.0)
        premature_stop_rate = float(expectancy_validation.get("premature_stop_rate", 0.0) or 0.0)
        avg_quality_score = float(expectancy_validation.get("avg_quality_score", 0.0) or 0.0)
        profile = cls._category_profile(category)
        min_avg_r = float(profile.get("expectancy_min_avg_r", GOVERNANCE_EXPECTANCY_MIN_AVG_R) or GOVERNANCE_EXPECTANCY_MIN_AVG_R)

        if scope == "unavailable":
            return "", "execution expectancy unavailable"
        if scope == "bootstrap":
            return "", "execution expectancy bootstrap: no closed-trade history yet"
        if scope == "category_context":
            return "", (
                f"execution expectancy bootstrap: no asset trade history yet; "
                f"category context sample_count={sample_count}"
            )
        if sample_count < GOVERNANCE_EXPECTANCY_MIN_SAMPLES:
            return "", (
                f"execution expectancy bootstrap: {sample_count}/{GOVERNANCE_EXPECTANCY_MIN_SAMPLES} "
                f"closed trades"
            )
        if avg_rr_realized < min_avg_r:
            return (
                f"live asset expectancy {avg_rr_realized:.2f}R below minimum "
                f"{min_avg_r:.2f}R",
                "",
            )
        if (
            target_hit_rate < GOVERNANCE_EXPECTANCY_MIN_TARGET_HIT_RATE
            and premature_stop_rate > GOVERNANCE_EXPECTANCY_MAX_PREMATURE_STOP_RATE
        ):
            return (
                f"live asset execution quality weak: target_hit_rate {target_hit_rate * 100:.1f}% "
                f"and premature_stop_rate {premature_stop_rate * 100:.1f}%",
                "",
            )
        if avg_quality_score < GOVERNANCE_EXPECTANCY_MIN_QUALITY_SCORE:
            return "", (
                f"live asset execution quality {avg_quality_score:.1f} below preferred "
                f"{GOVERNANCE_EXPECTANCY_MIN_QUALITY_SCORE:.1f}"
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
            live_validation_scope=str((context.get("live_validation") or {}).get("scope", "asset") or "asset"),
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
        if int(global_stats.get("total", 0) or 0) > 0:
            return {
                "scope": "portfolio_context",
                "total": 0,
                "accuracy_pct": 0.0,
                "portfolio_total": int(global_stats.get("total", 0) or 0),
                "portfolio_accuracy_pct": float(global_stats.get("accuracy_pct", 0.0) or 0.0),
            }
        return {"scope": "bootstrap", "total": 0, "accuracy_pct": 0.0}

    @staticmethod
    def _get_registry_validation(asset: str, category: str) -> Dict[str, Any]:
        return {
            "required": False,
            "asset_required": False,
            "bootstrap_mode": False,
            "empty_registry": False,
            "registry_count": 0,
            "asset": asset,
            "category": category,
            "matched": False,
            "exact_match": False,
            "match_scope": "playbook_only" if PLAYBOOK_ONLY_RUNTIME else "registry_removed",
            "strategies": [],
            "names": [],
        }

    @staticmethod
    def _get_expectancy_validation(asset: str, category: str) -> Dict[str, Any]:
        try:
            from services.execution_feedback_service import get_service as get_execution_feedback_service

            service = get_execution_feedback_service()
            lookback_days = max(90, GOVERNANCE_VALIDATION_DAYS * 4)
            asset_summary = service.summarize_history(
                asset=asset,
                category=category,
                days_back=lookback_days,
                limit=220,
            )
            if int(asset_summary.get("sample_count", 0) or 0) > 0:
                return {"scope": "asset", **asset_summary}

            category_summary = service.summarize_history(
                asset="",
                category=category,
                days_back=lookback_days,
                limit=500,
            )
            if int(category_summary.get("sample_count", 0) or 0) > 0:
                return {"scope": "category_context", **category_summary}
            return {"scope": "bootstrap", "sample_count": 0}
        except Exception as exc:
            logger.debug(f"[SignalGovernance] Execution expectancy unavailable: {exc}")
            return {"scope": "unavailable", "sample_count": 0}


signal_governance = SignalGovernance()
