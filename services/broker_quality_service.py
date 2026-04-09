from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from config.config import SPREAD_THRESHOLDS
from data.cache import Cache
from services.market_data_router import is_ig_primary_category
from utils.logger import get_logger

logger = get_logger()

_quote_cache = Cache(default_ttl=8)


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value or 0.0)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


def _normalize_provider(provider: str) -> str:
    token = str(provider or "").strip().lower()
    if token.startswith("ig"):
        return "ig"
    if token.startswith("deriv"):
        return "deriv"
    if token.startswith("binance"):
        return "binance"
    return token


def _provider_label(provider: str) -> str:
    mapping = {
        "ig": "IG",
        "deriv": "Deriv",
        "binance": "Binance",
    }
    return mapping.get(_normalize_provider(provider), str(provider or "").upper() or "Unknown")


def _metadata_age_seconds(meta: Dict[str, Any]) -> Optional[float]:
    if not isinstance(meta, dict) or not meta:
        return None
    if meta.get("live_age_seconds") is not None:
        return max(0.0, _safe_float(meta.get("live_age_seconds"), 0.0))
    raw = meta.get("as_of_utc")
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds())
    except Exception:
        return None


class BrokerQualityService:
    def __init__(self) -> None:
        self._last_market_states: Dict[str, str] = {}

    def _expected_primary_provider(self, category: str) -> str:
        normalized = str(category or "").strip().lower()
        if is_ig_primary_category(normalized):
            return "ig"
        return "deriv"

    def _comparison_provider(self, category: str, actual_provider: str) -> Optional[str]:
        normalized = str(category or "").strip().lower()
        actual = _normalize_provider(actual_provider)
        if is_ig_primary_category(normalized):
            return "deriv" if actual != "deriv" else None
        if normalized == "crypto":
            return "binance" if actual != "binance" else None
        return None

    def _cached_provider_quote(
        self,
        fetcher: Any,
        asset: str,
        category: str,
        provider: str,
    ) -> Tuple[Optional[float], Optional[float], Dict[str, Any]]:
        cache_key = f"broker_quality:{provider}:{asset}:{category}"
        cached = _quote_cache.get(cache_key)
        if cached is not None:
            price, spread, meta = cached
            return price, spread, dict(meta or {})
        try:
            price, spread, meta = fetcher.get_provider_quote(asset, category, provider)
            if price is not None:
                _quote_cache.set(cache_key, (float(price), float(spread or 0.0), dict(meta or {})))
            return price, spread, dict(meta or {})
        except Exception as exc:
            logger.debug(f"[BrokerQuality] provider quote {provider} {asset}: {exc}")
            return None, None, {}

    @staticmethod
    def _spread_profile(price: float, spread: float, category: str) -> Dict[str, Any]:
        threshold_pct = _safe_float(SPREAD_THRESHOLDS.get(str(category or "").lower(), 0.01), 0.01)
        if price <= 0.0:
            return {
                "spread_bps": 0.0,
                "spread_pct": 0.0,
                "spread_ratio_to_threshold": 0.0,
                "spread_regime": "unknown",
                "spread_quality_score": 0.55,
            }

        spread_pct = max(0.0, spread) / price if price else 0.0
        spread_bps = round(spread_pct * 10000.0, 3)
        ratio = spread_pct / threshold_pct if threshold_pct > 0 else 0.0

        if ratio <= 0.25:
            regime = "tight"
            quality = 1.0
        elif ratio <= 0.60:
            regime = "normal"
            quality = 0.85
        elif ratio <= 1.00:
            regime = "wide"
            quality = 0.62
        elif ratio <= 1.35:
            regime = "stressed"
            quality = 0.35
        else:
            regime = "extreme"
            quality = 0.10

        return {
            "spread_bps": spread_bps,
            "spread_pct": round(spread_pct, 6),
            "spread_ratio_to_threshold": round(ratio, 4),
            "spread_regime": regime,
            "spread_quality_score": round(quality, 4),
        }

    @staticmethod
    def _freshness_profile(meta: Dict[str, Any]) -> Dict[str, Any]:
        age_seconds = _metadata_age_seconds(meta)
        delayed = bool((meta or {}).get("delayed"))
        realtime = bool((meta or {}).get("realtime", False))
        from_cache = bool((meta or {}).get("from_cache", False))
        source_class = str((meta or {}).get("source_class") or "").lower()

        if delayed:
            state = "delayed"
            score = 0.20
        elif age_seconds is None:
            state = "unknown"
            score = 0.55 if realtime else 0.45
        elif source_class == "stream":
            if age_seconds <= 2.0:
                state = "fresh"
                score = 1.00
            elif age_seconds <= 5.0:
                state = "healthy"
                score = 0.88
            elif age_seconds <= 12.0:
                state = "aging"
                score = 0.64
            else:
                state = "stale"
                score = 0.22
        else:
            if age_seconds <= 5.0:
                state = "fresh"
                score = 0.82 if realtime else 0.62
            elif age_seconds <= 15.0:
                state = "healthy"
                score = 0.70 if realtime else 0.52
            elif age_seconds <= 30.0:
                state = "aging"
                score = 0.52 if realtime else 0.38
            else:
                state = "stale"
                score = 0.20

        if from_cache and state not in {"delayed", "stale"}:
            score = max(0.15, score - 0.08)

        return {
            "quote_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
            "quote_quality_state": state,
            "quote_quality_score": round(_clip(score), 4),
        }

    def _market_state_profile(
        self,
        fetcher: Any,
        asset: str,
        category: str,
        provider: str,
        meta: Dict[str, Any],
        fallback_status: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        provider_key = _normalize_provider(provider)
        try:
            status = fallback_status or fetcher.get_provider_market_status(asset, category, provider_key)
        except Exception:
            status = fallback_status or None

        raw_status = ""
        market_open = None
        if isinstance(status, dict):
            market_open = status.get("market_open")
            raw_status = str(status.get("ig_market_status") or status.get("status") or "").upper()

        if not raw_status and provider_key == "ig":
            raw_status = str((meta or {}).get("ig_market_status") or (meta or {}).get("market_status") or "").upper()

        if raw_status == "TRADEABLE":
            state = "TRADEABLE"
            quality = 1.00
        elif raw_status == "DEAL_NO_EDIT":
            state = "DEAL_NO_EDIT"
            quality = 0.82
        elif raw_status == "EDITS_ONLY":
            state = "EDITS_ONLY"
            quality = 0.38
        elif raw_status:
            state = raw_status
            quality = 0.25
        else:
            if market_open is True:
                state = "OPEN"
                quality = 0.92
            elif market_open is False:
                state = "CLOSED"
                quality = 0.28
            else:
                state = "UNKNOWN"
                quality = 0.55

        transition_key = f"{provider_key}:{asset}"
        previous_state = self._last_market_states.get(transition_key)
        changed = bool(previous_state and previous_state != state)
        transition = f"{previous_state}->{state}" if changed else ""
        transition_risk = 0.0
        if changed:
            if previous_state in {"TRADEABLE", "DEAL_NO_EDIT", "OPEN"} and state in {"EDITS_ONLY", "CLOSED", "UNKNOWN"}:
                transition_risk = 0.85
            elif previous_state in {"CLOSED", "UNKNOWN", "EDITS_ONLY"} and state in {"TRADEABLE", "DEAL_NO_EDIT", "OPEN"}:
                transition_risk = 0.35
            else:
                transition_risk = 0.20
        self._last_market_states[transition_key] = state

        return {
            "market_open": bool(market_open) if market_open is not None else state in {"TRADEABLE", "DEAL_NO_EDIT", "OPEN"},
            "market_state": state,
            "market_state_changed": changed,
            "market_state_transition": transition,
            "market_transition_risk": round(_clip(transition_risk), 4),
            "market_state_quality": round(_clip(quality), 4),
        }

    @staticmethod
    def _agreement_profile(
        price: float,
        comparison_price: Optional[float],
        category: str,
    ) -> Dict[str, Any]:
        if price <= 0.0 or comparison_price is None or comparison_price <= 0.0:
            return {
                "quote_agreement_bps": None,
                "quote_agreement_score": None,
                "quote_agreement_state": "unconfirmed",
            }

        divergence_pct = abs(float(price) - float(comparison_price)) / max(abs(float(price)), 1e-9)
        divergence_bps = divergence_pct * 10000.0
        threshold_pct = _safe_float(SPREAD_THRESHOLDS.get(str(category or "").lower(), 0.01), 0.01)
        expected_pct = max(0.00015, threshold_pct * 0.35)
        ratio = divergence_pct / expected_pct if expected_pct > 0 else 0.0
        agreement_score = _clip(1.0 - ratio)

        if ratio <= 0.35:
            state = "strong"
        elif ratio <= 0.75:
            state = "aligned"
        elif ratio <= 1.10:
            state = "divergent"
        else:
            state = "severe_divergence"

        return {
            "quote_agreement_bps": round(divergence_bps, 3),
            "quote_agreement_score": round(agreement_score, 4),
            "quote_agreement_state": state,
        }

    def build_snapshot(
        self,
        *,
        asset: str,
        category: str,
        fetcher: Any,
        primary_price: Optional[float] = None,
        primary_spread: Optional[float] = None,
        primary_meta: Optional[Dict[str, Any]] = None,
        market_status: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if fetcher is None:
            return {}

        primary_meta = dict(primary_meta or {})
        price = _safe_float(primary_price, 0.0)
        spread = max(0.0, _safe_float(primary_spread, 0.0))
        expected_primary = self._expected_primary_provider(category)
        actual_provider = _normalize_provider(primary_meta.get("source") or expected_primary)
        comparison_provider = self._comparison_provider(category, actual_provider)

        spread_profile = self._spread_profile(price, spread, category)
        freshness_profile = self._freshness_profile(primary_meta)
        state_profile = self._market_state_profile(
            fetcher,
            asset,
            category,
            actual_provider,
            primary_meta,
            fallback_status=market_status,
        )
        comparison = self._comparison_snapshot(fetcher, asset, category, comparison_provider)
        agreement_profile = self._agreement_profile(price, comparison["comparison_price"], category)
        broker_score = self._broker_score(spread_profile, freshness_profile, state_profile, agreement_profile)
        notes = self._broker_notes(
            expected_primary,
            actual_provider,
            spread_profile,
            freshness_profile,
            state_profile,
            agreement_profile,
            comparison["comparison_status"],
        )

        return {
            "expected_primary_provider": _provider_label(expected_primary),
            "primary_provider": _provider_label(actual_provider),
            "comparison_provider": _provider_label(comparison_provider) if comparison_provider else "",
            "fallback_active": actual_provider != expected_primary,
            "price": round(price, 8) if price > 0 else 0.0,
            "comparison_price": round(float(comparison["comparison_price"]), 8) if comparison["comparison_price"] is not None else None,
            "comparison_spread": round(float(comparison["comparison_spread"] or 0.0), 8) if comparison["comparison_spread"] is not None else None,
            "score": round(broker_score, 4),
            **spread_profile,
            **freshness_profile,
            **state_profile,
            **agreement_profile,
            "comparison_meta": dict(comparison["comparison_meta"] or {}),
            "notes": notes,
        }

    def _comparison_snapshot(
        self,
        fetcher: Any,
        asset: str,
        category: str,
        comparison_provider: Optional[str],
    ) -> Dict[str, Any]:
        comparison_price = None
        comparison_spread = None
        comparison_meta: Dict[str, Any] = {}
        comparison_status: Optional[Dict[str, Any]] = None
        if comparison_provider:
            comparison_price, comparison_spread, comparison_meta = self._cached_provider_quote(
                fetcher,
                asset,
                category,
                comparison_provider,
            )
            try:
                comparison_status = fetcher.get_provider_market_status(asset, category, comparison_provider)
            except Exception:
                comparison_status = None
        return {
            "comparison_price": comparison_price,
            "comparison_spread": comparison_spread,
            "comparison_meta": comparison_meta,
            "comparison_status": comparison_status,
        }

    @staticmethod
    def _broker_score(
        spread_profile: Dict[str, Any],
        freshness_profile: Dict[str, Any],
        state_profile: Dict[str, Any],
        agreement_profile: Dict[str, Any],
    ) -> float:
        weighted_score = (
            freshness_profile["quote_quality_score"] * 0.35
            + spread_profile["spread_quality_score"] * 0.25
            + state_profile["market_state_quality"] * 0.15
        )
        weight_total = 0.75
        agreement_score = agreement_profile.get("quote_agreement_score")
        if agreement_score is not None:
            weighted_score += float(agreement_score) * 0.25
            weight_total += 0.25

        broker_score = _clip(weighted_score / max(weight_total, 1e-9))
        if agreement_profile.get("quote_agreement_state") == "severe_divergence":
            broker_score = min(broker_score, 0.30)
        if freshness_profile.get("quote_quality_state") == "stale":
            broker_score = min(broker_score, 0.42)
        if state_profile.get("market_transition_risk", 0.0) >= 0.80:
            broker_score = min(broker_score, 0.35)
        return broker_score

    @staticmethod
    def _broker_notes(
        expected_primary: str,
        actual_provider: str,
        spread_profile: Dict[str, Any],
        freshness_profile: Dict[str, Any],
        state_profile: Dict[str, Any],
        agreement_profile: Dict[str, Any],
        comparison_status: Optional[Dict[str, Any]],
    ) -> List[str]:
        notes = []
        if actual_provider != expected_primary:
            notes.append("provider_fallback_active")
        if agreement_profile["quote_agreement_state"] == "strong":
            notes.append("broker_confirmed")
        elif agreement_profile["quote_agreement_state"] == "aligned":
            notes.append("broker_aligned")
        elif agreement_profile["quote_agreement_state"] == "divergent":
            notes.append("broker_divergence")
        elif agreement_profile["quote_agreement_state"] == "severe_divergence":
            notes.append("broker_severe_divergence")
        if spread_profile["spread_regime"] in {"wide", "stressed", "extreme"}:
            notes.append(f"spread_{spread_profile['spread_regime']}")
        if freshness_profile["quote_quality_state"] in {"aging", "stale", "delayed"}:
            notes.append(f"quote_{freshness_profile['quote_quality_state']}")
        if state_profile["market_state_changed"]:
            notes.append("market_state_changed")
        if state_profile["market_transition_risk"] >= 0.65:
            notes.append("market_transition_risk")
        if isinstance(comparison_status, dict):
            comparison_open = comparison_status.get("market_open")
            if comparison_open is not None and bool(comparison_open) != bool(state_profile.get("market_open")):
                notes.append("cross_broker_market_state_mismatch")
        return notes

_service = BrokerQualityService()


def get_service() -> BrokerQualityService:
    return _service

