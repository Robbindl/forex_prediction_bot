from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.asset_profiles import classify_depth_feed, get_depth_feed_policy


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = 0.0
    return max(low, min(high, numeric))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _active_session(*, category: str = "") -> str:
    now = _utc_now()
    hour = now.hour
    weekday = now.weekday()
    category_key = str(category or "").strip().lower()
    if category_key == "crypto":
        if 0 <= hour < 6:
            return "asia_core"
        if 6 <= hour < 8:
            return "europe_open"
        if 8 <= hour < 14:
            return "europe_core"
        if 14 <= hour < 16:
            return "us_overlap"
        if 16 <= hour < 19:
            return "us_open"
        return "us_core"
    if weekday == 5 or (weekday == 6 and hour < 22) or (weekday == 4 and hour >= 22):
        return "off"
    if 0 <= hour < 6:
        return "asia_core"
    if 6 <= hour < 8:
        return "europe_open"
    if 8 <= hour < 13:
        return "europe_core"
    if 13 <= hour < 15:
        return "us_overlap"
    if 15 <= hour < 17:
        return "us_open"
    if 17 <= hour < 22:
        return "us_core"
    return "off"


def _session_matches(current: str, allowed: str) -> bool:
    current_key = str(current or "").strip().lower()
    allowed_key = str(allowed or "").strip().lower()
    if not allowed_key or allowed_key == "all":
        return True
    if allowed_key == current_key:
        return True
    if allowed_key == "asia":
        return current_key.startswith("asia")
    if allowed_key == "europe":
        return current_key.startswith("europe")
    if allowed_key == "us":
        return current_key.startswith("us")
    return False


def _playbook_direction_sign(direction: str) -> int:
    token = str(direction or "").strip().upper()
    if token == "BUY":
        return 1
    if token == "SELL":
        return -1
    return 0


def _trend_sign(state: Any) -> int:
    token = str(state or "").strip().lower()
    if token in {"up", "bull", "bullish", "buy", "long", "trending_up"}:
        return 1
    if token in {"down", "bear", "bearish", "sell", "short", "trending_down"}:
        return -1
    return 0


def _direction_label(sign: int) -> str:
    if sign > 0:
        return "BUY"
    if sign < 0:
        return "SELL"
    return ""


def _trusted_context_depth_direction(context: Optional[Dict[str, Any]]) -> int:
    ctx = context or {}
    micro = dict(ctx.get("market_microstructure") or {})
    pressure, ready = _trusted_depth_pressure_score(
        micro,
        asset=str(ctx.get("asset") or micro.get("asset") or ""),
        category=str(ctx.get("category") or micro.get("category") or ""),
    )
    if not ready or abs(pressure) < 0.24:
        return 0
    return 1 if pressure > 0.0 else -1


def _trusted_depth_pressure_score(
    micro: Dict[str, Any],
    *,
    asset: str = "",
    category: str = "",
) -> Tuple[float, bool]:
    depth_available = bool(micro.get("depth_available"))
    synthetic = bool(micro.get("synthetic_depth_available") or micro.get("synthetic_depth"))
    try:
        levels = int(
            micro.get("depth_levels")
            or max(
                _safe_int(micro.get("bid_level_count") or micro.get("visible_bid_levels"), 0),
                _safe_int(micro.get("ask_level_count") or micro.get("visible_ask_levels"), 0),
            )
            or 0
        )
    except Exception:
        levels = 0
    mode = str(micro.get("depth_update_mode") or "").strip().lower()
    fidelity = str(micro.get("dom_source_fidelity") or "").strip().lower()
    feed_class = str(micro.get("depth_feed_class") or "").strip().lower()
    if not feed_class:
        feed_class = classify_depth_feed(
            asset=asset or str(micro.get("asset") or ""),
            category=category or str(micro.get("category") or ""),
            provider=str(micro.get("depth_provider") or micro.get("provider") or micro.get("exchange") or ""),
            provider_class=str(micro.get("depth_provider_class") or micro.get("source_class") or ""),
            source=str(micro.get("microstructure_source") or micro.get("source") or ""),
            depth_available=depth_available,
            synthetic_depth=synthetic,
            levels=levels,
        )
    policy = get_depth_feed_policy(asset or str(micro.get("asset") or ""), category or str(micro.get("category") or ""), feed_class)
    feed_class = str(policy.get("depth_feed_class") or feed_class)
    required_levels = int(policy.get("min_levels", 2) or 2)
    level_requirement_met = levels >= required_levels
    if feed_class == "exchange_deep":
        level_requirement_met = level_requirement_met or mode in _REAL_DEPTH_MODES or fidelity in {"event_ladder", "snapshot_depth", "stream_snapshot"}
    true_depth = bool(
        depth_available
        and not synthetic
        and feed_class not in {"quote_only", "synthetic"}
        and level_requirement_met
        and _safe_float(micro.get("depth_quality"), 0.0) >= _safe_float(policy.get("min_quality"), 0.24)
        and _safe_float(micro.get("depth_provider_trust_score"), 0.0) >= _safe_float(policy.get("min_trust"), 0.50)
    )
    if not true_depth:
        return 0.0, False

    weighted: List[Tuple[float, float]] = [
        (_safe_float(micro.get("score"), 0.0), 0.16),
        (_safe_float(micro.get("microstructure_alignment"), 0.0), 0.14),
        (_safe_float(micro.get("book_imbalance"), 0.0), 0.20),
        (_safe_float(micro.get("orderflow_book_imbalance"), 0.0), 0.20),
        (_safe_float(micro.get("orderflow_score"), 0.0), 0.14),
        (_safe_float(micro.get("trade_flow_score"), 0.0), 0.16),
        (_safe_float(micro.get("trade_delta_ratio"), 0.0), 0.08),
        (_safe_float(micro.get("tick_imbalance"), 0.0) * 0.75, 0.06),
    ]
    present = [(value, weight) for value, weight in weighted if abs(value) > 1e-9]
    if not present:
        return 0.0, True
    weight_total = sum(weight for _, weight in present)
    pressure = sum(value * weight for value, weight in present) / max(weight_total, 1e-9)
    strongest = max((value for value, _ in present), key=lambda value: abs(value))
    if abs(pressure) < 0.18 and abs(strongest) >= 0.45:
        same_side = sum(1 for value, _ in present if value * strongest > 0 and abs(value) >= 0.14)
        opposite_side = sum(1 for value, _ in present if value * strongest < 0 and abs(value) >= 0.24)
        if same_side >= max(1, opposite_side):
            pressure = strongest * 0.65
    return _clip(pressure, -1.0, 1.0), True


def _structure_direction(structure: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> str:
    bias = str(structure.get("structure_bias") or "").strip().lower()
    if bias == "buy":
        return "BUY"
    if bias == "sell":
        return "SELL"

    family = str(structure.get("pattern_family") or "").strip().lower()
    if "trending_up" in family:
        return "BUY"
    if "trending_down" in family:
        return "SELL"

    trend_sign = _trend_sign(structure.get("trend_15m")) or _trend_sign(structure.get("trend_1h")) or _trend_sign(structure.get("regime"))
    alignment = _safe_float(structure.get("alignment_score"), 0.0)
    setup = _safe_float(structure.get("setup_quality"), 0.0)
    if trend_sign and alignment >= 0.52 and setup >= 0.48:
        return _direction_label(trend_sign)

    ctx = context if isinstance(context, dict) else {}
    depth_sign = _trusted_context_depth_direction(ctx)
    if depth_sign:
        return _direction_label(depth_sign)
    micro = dict(ctx.get("market_microstructure") or {})
    micro_score = _safe_float(micro.get("score", micro.get("book_imbalance")), 0.0)
    if abs(micro_score) >= 0.55 and alignment >= 0.50 and setup >= 0.45:
        return _direction_label(1 if micro_score > 0 else -1)
    return ""


def _best_rejected_reason(records: List[Dict[str, Any]]) -> str:
    if not records:
        return ""
    ordered = sorted(
        records,
        key=lambda row: (
            _safe_float(row.get("confidence"), 0.0),
            _safe_float(row.get("score"), 0.0),
            len(str(row.get("reason") or "")),
        ),
        reverse=True,
    )
    return str(ordered[0].get("reason") or "")


@dataclass(frozen=True)
class _PlaybookProfile:
    seed_floor: float
    setup_floor: float
    alignment_floor: float
    preferred_interval: str
    allowed_sessions: Tuple[str, ...]
    runner_target_rr: float = 2.0
    trail_activation_rr: float = 1.10
    trail_atr_multiple: float = 1.05


@dataclass(frozen=True)
class _AssetPlaybookPlan:
    playbooks: Tuple[str, ...]
    allowed_sessions: Tuple[str, ...] = ()
    preferred_interval: str = ""
    early_inflection_enabled: bool = False


_DEFAULT_PLAYBOOKS = (
    "breakout_continuation",
    "breakout_retest",
    "trend_pullback",
    "failed_break_reclaim",
    "reversal_exhaustion",
    "aggressive_expansion",
)

_PROFILES: Dict[str, _PlaybookProfile] = {
    "crypto": _PlaybookProfile(0.58, 0.48, 0.46, "5m", ("asia", "europe", "us"), 2.6, 1.20, 1.12),
    "forex": _PlaybookProfile(0.56, 0.52, 0.52, "15m", ("asia", "europe", "us"), 1.6, 0.85, 0.75),
    "commodities": _PlaybookProfile(0.57, 0.50, 0.50, "15m", ("europe", "us"), 2.1, 1.12, 1.05),
    "indices": _PlaybookProfile(0.57, 0.50, 0.50, "15m", ("us",), 2.0, 1.10, 1.05),
    "default": _PlaybookProfile(0.58, 0.52, 0.52, "15m", ("asia", "europe", "us")),
}

_ASSET_PLANS: Dict[str, _AssetPlaybookPlan] = {
    "BTC-USD": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="5m"),
    "ETH-USD": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="5m"),
    "SOL-USD": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="15m"),
    "BNB-USD": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="15m"),
    "XRP-USD": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="15m"),
    "WTI": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("us_overlap", "us_open", "us_core"), "15m"),
    "USOIL": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("us_overlap", "us_open", "us_core"), "15m"),
    "US500": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("us_overlap", "us_open", "us_core"), "15m", True),
    "US100": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("us_overlap", "us_open", "us_core"), "15m", True),
    "US30": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("us_overlap", "us_open", "us_core"), "15m", True),
    "UK100": _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, ("europe_open", "europe_core"), "15m"),
}

_REAL_DEPTH_MODES = {
    "live",
    "event_stream",
    "ladder_stream",
    "delta_stream",
    "depth_stream",
    "snapshot_poll",
    "stream_snapshot",
    "snapshot_stream",
}

_FALSE_DEPTH_MODES = {"", "none", "synthetic", "top_quote", "top_of_book"}


class PlaybookService:
    def _profile(self, category: str) -> _PlaybookProfile:
        return _PROFILES.get(str(category or "").strip().lower(), _PROFILES["default"])

    def _asset_plan(self, asset: str, category: str) -> _AssetPlaybookPlan:
        canonical = str(asset or "").strip().upper()
        if canonical in _ASSET_PLANS:
            return _ASSET_PLANS[canonical]
        if canonical.endswith("/JPY") and str(category or "").strip().lower() == "forex":
            return _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS, preferred_interval="5m", early_inflection_enabled=True)
        return _AssetPlaybookPlan(_DEFAULT_PLAYBOOKS)

    def preferred_interval(self, category: str, asset: str = "") -> str:
        plan = self._asset_plan(asset, category)
        if plan.preferred_interval:
            return plan.preferred_interval
        return self._profile(category).preferred_interval

    def _default_allowed_sessions(
        self,
        asset: str,
        category: str,
        profile: _PlaybookProfile,
        plan: _AssetPlaybookPlan,
    ) -> Tuple[str, ...]:
        if plan.allowed_sessions:
            return plan.allowed_sessions
        category_key = str(category or "").strip().lower()
        canonical = str(asset or "").strip().upper()
        if category_key == "indices":
            return ("europe_open", "europe_core") if canonical == "UK100" else ("us_overlap", "us_open", "us_core")
        return profile.allowed_sessions

    def _session_allowed(self, asset: str, category: str) -> Tuple[bool, str, Tuple[str, ...]]:
        profile = self._profile(category)
        plan = self._asset_plan(asset, category)
        session = _active_session(category=category)
        allowed = self._default_allowed_sessions(asset, category, profile, plan)
        if not allowed:
            return True, session, allowed
        return any(_session_matches(session, item) for item in allowed), session, allowed

    def _management_template(
        self,
        profile: _PlaybookProfile,
        playbook: str,
        *,
        asset: str,
        category: str,
    ) -> Dict[str, Any]:
        category_key = str(category or "").strip().lower()
        partial_rr = {
            "crypto": [1.30, 2.10],
            "forex": [0.85, 1.25],
            "commodities": [1.20, 1.90],
            "indices": [1.15, 1.80],
        }.get(category_key, [1.15, 1.75])
        if playbook in {"reversal_exhaustion", "failed_break_reclaim"}:
            partial_rr = [0.75, 1.10] if category_key == "forex" else [1.00, 1.55]
        return {
            "style": "clean_depth_playbook",
            "playbook": playbook,
            "asset": str(asset or "").strip().upper(),
            "category": category_key,
            "partial_take_profit_rr": partial_rr,
            "partial_take_profit_size_fractions": [0.30, 0.40, 0.30],
            "runner_target_rr": round(float(profile.runner_target_rr), 4),
            "trail_activation_rr": round(float(profile.trail_activation_rr), 4),
            "trail_atr_multiple": round(float(profile.trail_atr_multiple), 4),
            "trail_mode": "extreme_atr",
            "break_even_after_partial": True,
            "preferred_interval": profile.preferred_interval,
        }

    def _context_directional_confluence(self, context: Optional[Dict[str, Any]], direction: str) -> Dict[str, Any]:
        ctx = dict(context) if isinstance(context, dict) else {}
        direction_sign = _playbook_direction_sign(direction)
        if direction_sign == 0:
            return self._empty_context(direction="")

        cross = dict(ctx.get("cross_asset_context") or {})
        micro = dict(ctx.get("market_microstructure") or {})

        cross_score = _safe_float(cross.get("score"), 0.0)
        cross_state = str(cross.get("state") or "").strip().lower()
        if abs(cross_score) <= 1e-9:
            if cross_state in {"supportive", "buy_support"}:
                cross_score = 0.30
            elif cross_state == "sell_support":
                cross_score = -0.30
            elif cross_state == "conflicted":
                cross_score = -0.20
        cross_support = _clip(cross_score * direction_sign, -1.0, 1.0)
        cross_confidence = _clip(_safe_float(cross.get("confidence"), 0.0), 0.0, 1.0)

        pressure_score, pressure_ready = _trusted_depth_pressure_score(
            micro,
            asset=str(ctx.get("asset") or micro.get("asset") or ""),
            category=str(ctx.get("category") or micro.get("category") or ""),
        )
        if pressure_ready:
            micro_support = _clip(pressure_score * direction_sign, -1.0, 1.0)
        else:
            raw_micro_values = [
                _safe_float(micro.get("score"), 0.0),
                _safe_float(micro.get("book_imbalance"), 0.0),
                _safe_float(micro.get("trade_flow_score"), 0.0),
                _safe_float(micro.get("orderflow_score"), 0.0),
                _safe_float(micro.get("orderflow_book_imbalance"), 0.0),
                _safe_float(micro.get("tick_imbalance"), 0.0) * 0.75,
                _safe_float(micro.get("velocity_bps"), 0.0) / 0.45,
            ]
            directional_values = [value * direction_sign for value in raw_micro_values if abs(value) > 1e-9]
            micro_support = _clip(max(directional_values, key=lambda value: (abs(value), value)), -1.0, 1.0) if directional_values else 0.0

        whale_dominant = str(ctx.get("whale_dominant") or "").strip().upper()
        whale_ratio = _clip(_safe_float(ctx.get("whale_ratio"), 0.0), 0.0, 1.0)
        whale_sign = 1 if whale_dominant == "BUY" else -1 if whale_dominant == "SELL" else 0
        whale_support = _clip(whale_sign * direction_sign * whale_ratio, -1.0, 1.0)

        support_components = sum(
            1
            for value, floor in (
                (cross_support, 0.10),
                (micro_support, 0.18),
                (whale_support, 0.16),
            )
            if value >= floor
        )
        conflict_components = sum(
            1
            for value, floor in (
                (cross_support, -0.18),
                (micro_support, -0.22),
                (whale_support, -0.20),
            )
            if value <= floor
        )
        score = _clip(
            max(cross_support, 0.0) * 0.24
            + max(micro_support, 0.0) * 0.58
            + max(whale_support, 0.0) * 0.18,
            0.0,
            1.0,
        )

        depth = self._depth_readiness(
            micro,
            asset=str(ctx.get("asset") or micro.get("asset") or ""),
            category=str(ctx.get("category") or micro.get("category") or ""),
        )
        return {
            "direction": direction,
            "score": round(score, 4),
            "cross_support": round(cross_support, 4),
            "micro_support": round(micro_support, 4),
            "whale_support": round(whale_support, 4),
            "cross_confidence": round(cross_confidence, 4),
            "support_components": support_components,
            "conflict_components": conflict_components,
            **depth,
        }

    def _empty_context(self, direction: str = "") -> Dict[str, Any]:
        return {
            "direction": direction,
            "score": 0.0,
            "cross_support": 0.0,
            "micro_support": 0.0,
            "whale_support": 0.0,
            "cross_confidence": 0.0,
            "support_components": 0,
            "conflict_components": 0,
            "depth_available": False,
            "synthetic_depth": False,
            "true_depth_ready": False,
            "true_depth_reason": "depth_unavailable",
            "depth_levels": 0,
            "depth_quality": 0.0,
            "depth_update_mode": "none",
            "depth_provider": "",
            "depth_provider_class": "",
            "depth_feed_class": "quote_only",
            "depth_normalization_scope": "",
            "depth_sovereignty_allowed": False,
            "depth_confirmation_override_allowed": False,
            "microstructure_source": "none",
            "dom_event_backed": False,
            "dom_ladder_ready": False,
            "dom_stream_snapshot_ready": False,
            "dom_source_fidelity": "none",
            "dom_authority_tier": "none",
        }

    @staticmethod
    def _depth_wait_reason(depth: Dict[str, Any]) -> str:
        reason = str(depth.get("true_depth_reason") or "").strip().lower()
        if reason in {"", "ready"}:
            return ""
        return reason

    def _depth_readiness(
        self,
        micro: Dict[str, Any],
        *,
        asset: str = "",
        category: str = "",
    ) -> Dict[str, Any]:
        mode = str(micro.get("depth_update_mode") or "").strip().lower()
        provider = str(micro.get("depth_provider") or micro.get("provider") or micro.get("exchange") or "").strip()
        provider_key = provider.lower()
        provider_class = str(micro.get("depth_provider_class") or micro.get("source_class") or "").strip().lower()
        source = str(micro.get("microstructure_source") or micro.get("source") or "").strip().lower() or "none"
        quality_tier = str(micro.get("depth_quality_tier") or "").strip().lower()
        depth_available = bool(micro.get("depth_available"))
        synthetic = bool(micro.get("synthetic_depth_available") or micro.get("synthetic_depth"))
        external_rejected = bool(micro.get("external_depth_rejected"))
        event_backed = bool(micro.get("dom_event_backed"))
        ladder_ready = bool(micro.get("dom_ladder_ready"))
        snapshot_ready = bool(micro.get("dom_stream_snapshot_ready"))
        source_fidelity = str(micro.get("dom_source_fidelity") or "").strip().lower() or "none"
        authority_tier = str(micro.get("dom_authority_tier") or "").strip().lower() or "none"
        levels = _safe_int(
            micro.get("depth_levels")
            or max(
                _safe_int(micro.get("bid_level_count") or micro.get("visible_bid_levels"), 0),
                _safe_int(micro.get("ask_level_count") or micro.get("visible_ask_levels"), 0),
            ),
            0,
        )
        feed_class = str(micro.get("depth_feed_class") or "").strip().lower()
        if not feed_class:
            feed_class = classify_depth_feed(
                asset=asset or str(micro.get("asset") or ""),
                category=category or str(micro.get("category") or ""),
                provider=provider,
                provider_class=provider_class,
                source=source,
                depth_available=depth_available,
                synthetic_depth=synthetic,
                levels=levels,
            )
        if feed_class == "exchange_deep" and levels <= 0 and depth_available and (
            mode in _REAL_DEPTH_MODES
            or event_backed
            or ladder_ready
            or snapshot_ready
            or source_fidelity in {"event_ladder", "snapshot_depth"}
        ):
            levels = 2
        if feed_class == "exchange_deep" and levels <= 0:
            levels = {"top_only": 1, "thin": 2, "partial": 4, "solid": 6, "strong": 8, "full": 10}.get(quality_tier, 0)
        feed_class = classify_depth_feed(
            asset=asset or str(micro.get("asset") or ""),
            category=category or str(micro.get("category") or ""),
            provider=provider,
            provider_class=provider_class,
            source=source,
            depth_available=depth_available,
            synthetic_depth=synthetic,
            levels=levels,
        )
        policy = get_depth_feed_policy(
            asset or str(micro.get("asset") or ""),
            category or str(micro.get("category") or ""),
            feed_class,
        )
        min_levels = _safe_int(policy.get("min_levels"), 2)
        min_quality = _safe_float(policy.get("min_quality"), 0.35)
        min_trust = _safe_float(policy.get("min_trust"), 0.52)
        quality = _clip(_safe_float(micro.get("depth_quality"), 0.0), 0.0, 1.0)
        if quality <= 0.0 and levels > 0:
            if mode in _REAL_DEPTH_MODES or event_backed or ladder_ready or snapshot_ready or source_fidelity in {"event_ladder", "snapshot_depth"}:
                quality = 0.55
            else:
                quality = _clip(levels / 10.0, 0.20, 1.0)
        trust = _clip(_safe_float(micro.get("depth_provider_trust_score"), 0.0), 0.0, 1.0)
        if trust <= 0.0:
            if feed_class == "exchange_deep" or provider_class == "exchange_depth" or any(token in provider_key for token in ("binance", "bybit", "okx")):
                trust = 0.86
            elif feed_class in {"broker_l2", "thin_broker_l2"} or provider_class in {"broker_l2", "sidecar", "redis_subscriber"} or any(token in provider_key for token in ("dukascopy", "ctrader", "orderflow")):
                trust = 0.72
            elif depth_available and (mode in _REAL_DEPTH_MODES or event_backed or ladder_ready or snapshot_ready):
                trust = 0.70
        quote_alignment = _clip(_safe_float(micro.get("depth_quote_alignment_score"), 1.0), 0.0, 1.0)
        agreement = str(micro.get("depth_quote_agreement_state") or "").strip().lower()

        reasons: List[str] = []
        if not depth_available:
            reasons.append("depth_unavailable")
        if synthetic:
            reasons.append("synthetic_depth")
        if feed_class in {"quote_only", "synthetic"}:
            reasons.append("depth_not_actionable")
        if external_rejected:
            reasons.append("external_depth_rejected")
        if mode in _FALSE_DEPTH_MODES:
            reasons.append("depth_mode_untrusted")
        if mode and mode not in _REAL_DEPTH_MODES and mode != "none":
            reasons.append("depth_mode_unknown")
        if levels < min_levels:
            reasons.append("depth_too_shallow")
        if quality < min_quality:
            reasons.append("depth_quality_low")
        if trust < min_trust:
            reasons.append("depth_trust_low")
        if quote_alignment < 0.70 or agreement in {"diverged", "conflict", "conflicted"}:
            reasons.append("depth_quote_misaligned")

        ready = not reasons
        return {
            "depth_available": depth_available,
            "synthetic_depth": synthetic,
            "true_depth_ready": ready,
            "true_depth_reason": "ready" if ready else ",".join(reasons),
            "depth_levels": levels,
            "depth_quality": round(quality, 4),
            "depth_update_mode": mode or "none",
            "depth_provider": provider,
            "depth_provider_class": provider_class,
            "depth_feed_class": str(policy.get("depth_feed_class") or feed_class),
            "depth_normalization_scope": str(
                micro.get("depth_normalization_scope")
                or f"{asset or micro.get('asset') or ''}:{provider or source}:{policy.get('depth_feed_class') or feed_class}"
            ),
            "depth_min_levels_required": min_levels,
            "depth_min_quality_required": round(min_quality, 4),
            "depth_min_trust_required": round(min_trust, 4),
            "depth_sovereignty_allowed": bool(policy.get("sovereignty_allowed")),
            "depth_confirmation_override_allowed": bool(policy.get("confirmation_override_allowed")),
            "microstructure_source": source,
            "dom_event_backed": event_backed,
            "dom_ladder_ready": ladder_ready,
            "dom_stream_snapshot_ready": snapshot_ready,
            "dom_source_fidelity": source_fidelity,
            "dom_authority_tier": authority_tier,
        }

    def _candidate(
        self,
        *,
        asset: str,
        category: str,
        session: str,
        playbook: str,
        direction: str,
        entry_style: str,
        structure: Dict[str, Any],
        context_profile: Dict[str, Any],
        score: float,
        notes: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        profile = self._profile(category)
        confidence = _clip(0.38 + score * 0.34 + context_profile.get("score", 0.0) * 0.16 + max(context_profile.get("micro_support", 0.0), 0.0) * 0.08, 0.0, 0.94)
        return {
            "playbook": playbook,
            "direction": direction,
            "score": round(_clip(score, 0.0, 1.0), 4),
            "confidence": round(confidence, 4),
            "entry_style": entry_style,
            "preferred_interval": self.preferred_interval(category, asset),
            "session": session,
            "management": self._management_template(profile, playbook, asset=asset, category=category),
            "context_confluence": round(_safe_float(context_profile.get("score"), 0.0), 4),
            "cross_alignment": round(_safe_float(context_profile.get("cross_support"), 0.0), 4),
            "cross_confidence": round(_safe_float(context_profile.get("cross_confidence"), 0.0), 4),
            "micro_score": round(_safe_float(context_profile.get("micro_support"), 0.0), 4),
            "whale_context_support": round(_safe_float(context_profile.get("whale_support"), 0.0), 4),
            "support_components": int(context_profile.get("support_components", 0) or 0),
            "conflict_components": int(context_profile.get("conflict_components", 0) or 0),
            "cross_context_support": round(_safe_float(context_profile.get("cross_support"), 0.0), 4),
            "micro_context_support": round(_safe_float(context_profile.get("micro_support"), 0.0), 4),
            "generic_flow_override": any("generic_flow_override" in str(note) for note in (notes or [])),
            "generic_flow_override_source": self._generic_flow_source(context_profile),
            "dom_stream_snapshot_ready": bool(context_profile.get("dom_stream_snapshot_ready")),
            "shock_score": 0.0,
            "shock_event_score": 0.0,
            "headline_shock_score": 0.0,
            "shock_displacement_score": 0.0,
            "shock_structure_score": 0.0,
            "shock_liquidity_score": 0.0,
            "shock_timing_score": 0.0,
            "shock_fresh_event": False,
            "shock_supported": False,
            "shock_event_label": "",
            "asset_plan": {},
            "htf_alignment": self._higher_timeframe_alignment(structure, direction),
            "notes": list(notes or []),
            "qualification": {},
        }

    def _generic_flow_source(self, context_profile: Dict[str, Any]) -> str:
        if not bool(context_profile.get("true_depth_ready")):
            return "flow"
        feed_class = str(context_profile.get("depth_feed_class") or "").strip().lower()
        if feed_class in {"broker_l2", "thin_broker_l2"}:
            return feed_class
        if bool(context_profile.get("dom_event_backed")) and bool(context_profile.get("dom_ladder_ready")):
            if str(context_profile.get("dom_authority_tier") or "") == "fragmented_event_ladder":
                return "fragmented_event_ladder"
            return "true_depth"
        if bool(context_profile.get("dom_stream_snapshot_ready")) or str(context_profile.get("dom_source_fidelity") or "") == "snapshot_depth":
            return "snapshot_depth"
        return "true_depth"

    def _higher_timeframe_alignment(self, structure: Dict[str, Any], direction: str) -> Dict[str, Any]:
        sign = _playbook_direction_sign(direction)
        trend_15m = _trend_sign(structure.get("trend_15m"))
        trend_1h = _trend_sign(structure.get("trend_1h"))
        return {
            "trend_15m": str(structure.get("trend_15m") or ""),
            "trend_1h": str(structure.get("trend_1h") or ""),
            "aligned_count": int(trend_15m == sign) + int(trend_1h == sign),
        }

    def _qualification_metrics(
        self,
        candidate: Dict[str, Any],
        structure: Dict[str, Any],
        context_profile: Dict[str, Any],
        category: str,
    ) -> Dict[str, Any]:
        direction = str(candidate.get("direction") or "")
        sign = _playbook_direction_sign(direction)
        playbook = str(candidate.get("playbook") or "")
        trend_15m = _trend_sign(structure.get("trend_15m"))
        trend_1h = _trend_sign(structure.get("trend_1h"))
        trend_5m = _trend_sign(structure.get("trend_5m"))
        htf_count = int(trend_15m == sign) + int(trend_1h == sign)
        depth_override = bool(context_profile.get("depth_confirmation_override_allowed"))
        required_trends = 1 if playbook != "trend_pullback" or depth_override else 2
        fast_override = bool(structure.get("fast_entry_confirmation_ready")) or (
            bool(structure.get("trigger_trend_aligned")) and _safe_int(structure.get("fast_entry_confirmation_count"), 0) >= 1
        )
        early_relief = bool(
            fast_override
            or bool(structure.get("trigger_trend_aligned"))
            or trend_5m == sign
            or depth_override
        )
        return {
            "effective_required_trends": required_trends,
            "aligned_htf_trends": htf_count,
            "allow_early_trend_relief": early_relief,
            "fast_confirmation_override": fast_override,
            "trigger_trend_aligned": bool(structure.get("trigger_trend_aligned")) or trend_5m == sign,
            "true_depth_ready": bool(context_profile.get("true_depth_ready")),
            "depth_confirmation_override_allowed": depth_override,
            "depth_feed_class": str(context_profile.get("depth_feed_class") or ""),
            "context_support_components": int(context_profile.get("support_components", 0) or 0),
            "context_conflict_components": int(context_profile.get("conflict_components", 0) or 0),
        }

    def _qualify_candidate(
        self,
        candidate: Dict[str, Any],
        *,
        asset: str,
        category: str,
        structure: Dict[str, Any],
        plan: _AssetPlaybookPlan,
        inactivity_profile: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[bool, str]:
        del plan, inactivity_profile
        structure = structure if isinstance(structure, dict) else {}
        direction = str(candidate.get("direction") or _structure_direction(structure, context)).upper()
        playbook = str(candidate.get("playbook") or "")
        context_profile = self._context_directional_confluence(context, direction)
        qualification = self._qualification_metrics(candidate, structure, context_profile, category)
        candidate["qualification"] = qualification

        sign = _playbook_direction_sign(direction)
        if sign == 0:
            return False, f"no_direction:{playbook}"
        if context_profile["conflict_components"] > 0 and context_profile["support_components"] == 0:
            return False, f"context_conflict:{playbook}"

        bias = str(structure.get("structure_bias") or "").strip().lower()
        family = str(structure.get("pattern_family") or "").strip().lower()
        alignment = _safe_float(structure.get("alignment_score"), 0.0)
        setup = _safe_float(structure.get("setup_quality"), 0.0)
        target = _safe_float(structure.get("target_efficiency_score"), 1.0)
        extension = _safe_float(structure.get("extension_score"), 0.0)
        impulse_age = _safe_int(structure.get("impulse_age_bars"), 0)
        cluster = _safe_float(structure.get("cluster_penalty"), 0.0)
        depth_override = bool(context_profile.get("depth_confirmation_override_allowed"))
        trigger_aligned = bool(qualification.get("trigger_trend_aligned"))

        if bias == "neutral" and not ("trending_up" in family or "trending_down" in family):
            return False, f"neutral_structure:{playbook}"
        if bias in {"buy", "sell"} and ((bias == "buy" and sign < 0) or (bias == "sell" and sign > 0)) and playbook not in {"early_inflection", "reversal_exhaustion", "failed_break_reclaim"}:
            return False, f"bias_conflict:{playbook}"
        if alignment < 0.36 or setup < 0.32:
            return False, f"setup_quality_too_weak:{playbook}"
        if cluster > 0.28:
            return False, f"cluster_risk:{playbook}"
        if playbook == "aggressive_expansion":
            if not bool(structure.get("entry_confirmation_ready")) and not depth_override:
                return False, "confirmation_pending:aggressive_expansion"
            if extension > (1.62 if depth_override else 1.35):
                return False, "entry_extended:aggressive_expansion"

        if target < 0.08:
            return False, f"target_space_too_thin:{playbook}"

        if playbook == "trend_pullback":
            if not bool(structure.get("first_pullback_ready")) and not bool(structure.get("entry_confirmation_ready")):
                return False, "pullback_missing:trend_pullback"
            if qualification["aligned_htf_trends"] < qualification["effective_required_trends"]:
                return False, "trend_misaligned:trend_pullback"

        if playbook in {"breakout_continuation", "breakout_retest"}:
            flow_override = bool(candidate.get("generic_flow_override")) or str(candidate.get("entry_style") or "") in {
                "elite_context_continuation",
                "elite_flow_continuation",
                "breakout_ignition",
            }
            if not (flow_override or trigger_aligned or qualification["aligned_htf_trends"] >= 1 or depth_override):
                return False, f"trend_misaligned:{playbook}"

        extension_relief = bool(
            str(candidate.get("entry_style") or "") in {"elite_context_continuation", "elite_flow_continuation", "breakout_ignition"}
            and context_profile["support_components"] >= 2
            and max(
                context_profile.get("micro_support", 0.0),
                context_profile.get("cross_support", 0.0),
                context_profile.get("whale_support", 0.0),
            )
            >= 0.24
        )
        if extension > 1.62 and not (depth_override or extension_relief):
            return False, f"entry_extended:{playbook}"
        if impulse_age > 16 and not (depth_override and context_profile["support_components"] >= 1):
            return False, f"setup_too_old:{playbook}"
        return True, ""

    def _elite_ready_continuation(
        self,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        plan: _AssetPlaybookPlan,
        inactivity_profile: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del inactivity_profile
        structure = structure if isinstance(structure, dict) else {}
        direction = _structure_direction(structure, context)
        if not direction:
            return None
        context_profile = self._context_directional_confluence(context, direction)
        sign = _playbook_direction_sign(direction)
        alignment = _safe_float(structure.get("alignment_score"), 0.0)
        setup = _safe_float(structure.get("setup_quality"), 0.0)
        target = _safe_float(structure.get("target_efficiency_score"), 1.0)
        extension = _safe_float(structure.get("extension_score"), 0.0)
        cluster = _safe_float(structure.get("cluster_penalty"), 0.0)
        family = str(structure.get("pattern_family") or "").strip().lower()
        trend_5m_aligned = _trend_sign(structure.get("trend_5m")) == sign
        trigger_aligned = bool(structure.get("trigger_trend_aligned")) or trend_5m_aligned
        depth_override = bool(context_profile.get("depth_confirmation_override_allowed"))
        support = int(context_profile.get("support_components", 0) or 0)
        conflict = int(context_profile.get("conflict_components", 0) or 0)
        flow = max(
            _safe_float(context_profile.get("micro_support"), 0.0),
            _safe_float(context_profile.get("cross_support"), 0.0),
            _safe_float(context_profile.get("whale_support"), 0.0),
        )
        if conflict > 0 or cluster > 0.24:
            return None
        if alignment < 0.50 or setup < 0.45 or target < 0.08:
            return None

        notes: List[str] = []
        entry_style = "elite_context_continuation"
        depth_available = bool(context_profile.get("depth_available"))
        if depth_override and support >= 1 and flow >= 0.20:
            source = self._generic_flow_source(context_profile)
            notes.append(f"generic_flow_{source}")
            notes.append(f"generic_flow_override={source}")
            entry_style = "elite_flow_continuation"
        elif not depth_available and support >= 2 and flow >= 0.24:
            notes.append("generic_flow_flow")
            notes.append("generic_flow_override=flow")
            entry_style = "elite_flow_continuation"
        elif support >= 2 and flow >= 0.24:
            entry_style = "elite_context_continuation"
        elif not (trigger_aligned or "trending" in family):
            return None

        depth_momentum = bool(
            depth_override
            and support >= 1
            and flow >= 0.55
            and alignment >= 0.50
            and setup >= 0.50
            and extension <= 1.62
        )
        if depth_momentum:
            entry_style = "breakout_ignition"
            notes.append("breakout_ignition=1")
            notes.append("depth_momentum_continuation=1")

        score = _clip(
            alignment * 0.30
            + setup * 0.28
            + max(target, 0.0) * 0.12
            + context_profile["score"] * 0.20
            + max(flow, 0.0) * 0.10,
            0.0,
            1.0,
        )
        candidate = self._candidate(
            asset=asset,
            category=category,
            session=session,
            playbook="breakout_continuation",
            direction=direction,
            entry_style=entry_style,
            structure=structure,
            context_profile=context_profile,
            score=score,
            notes=notes,
        )
        ok, _ = self._qualify_candidate(candidate, asset=asset, category=category, structure=structure, plan=plan, context=context)
        return candidate if ok else None

    def _elite_ready_fallback(self, **kwargs: Any) -> Optional[Dict[str, Any]]:
        return self._elite_ready_continuation(**kwargs)

    def _breakout_continuation(
        self,
        frame: Any,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del frame
        direction = _structure_direction(structure, context)
        if not direction:
            return None
        alignment = _safe_float(structure.get("alignment_score"), 0.0)
        setup = _safe_float(structure.get("setup_quality"), 0.0)
        breakout = abs(_safe_float(structure.get("breakout_score"), 0.0))
        if alignment < 0.48 or setup < 0.46:
            return None
        ctx = self._context_directional_confluence(context, direction)
        score = _clip(alignment * 0.34 + setup * 0.30 + breakout * 0.16 + ctx["score"] * 0.20, 0.0, 1.0)
        return self._candidate(asset=asset, category=category, session=session, playbook="breakout_continuation", direction=direction, entry_style="breakout_close", structure=structure, context_profile=ctx, score=score)

    def _breakout_retest(
        self,
        frame: Any,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del frame
        direction = _structure_direction(structure, context)
        if not direction:
            return None
        ready = bool(structure.get("breakout_retest_ready")) or (
            _safe_float(structure.get("alignment_score"), 0.0) >= 0.62
            and _safe_float(structure.get("setup_quality"), 0.0) >= 0.60
            and _safe_float(structure.get("distance_to_resistance" if direction == "BUY" else "distance_to_support"), 1.0) <= 0.0012
        )
        if not ready:
            return None
        ctx = self._context_directional_confluence(context, direction)
        score = _clip(_safe_float(structure.get("alignment_score"), 0.0) * 0.32 + _safe_float(structure.get("setup_quality"), 0.0) * 0.30 + 0.16 + ctx["score"] * 0.22, 0.0, 1.0)
        return self._candidate(asset=asset, category=category, session=session, playbook="breakout_retest", direction=direction, entry_style="retest_hold", structure=structure, context_profile=ctx, score=score)

    def _trend_pullback(
        self,
        frame: Any,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del frame, session
        direction = _structure_direction(structure, context)
        if not direction:
            return None
        sign = _playbook_direction_sign(direction)
        if direction == "BUY" and _safe_float(structure.get("upside_exhaustion_score"), 0.0) >= 0.65:
            return None
        if direction == "SELL" and _safe_float(structure.get("downside_exhaustion_score"), 0.0) >= 0.65:
            return None
        if not bool(structure.get("first_pullback_ready")) and not bool(structure.get("entry_confirmation_ready")):
            return None
        trend_15m = _trend_sign(structure.get("trend_15m"))
        trend_1h = _trend_sign(structure.get("trend_1h"))
        if trend_15m != sign and trend_1h != sign:
            return None
        ctx = self._context_directional_confluence(context, direction)
        pullback = abs(_safe_float(structure.get("pullback_score"), 0.0))
        score = _clip(_safe_float(structure.get("alignment_score"), 0.0) * 0.28 + _safe_float(structure.get("setup_quality"), 0.0) * 0.30 + pullback * 0.20 + ctx["score"] * 0.22, 0.0, 1.0)
        return self._candidate(asset=asset, category=category, session=_active_session(category=category), playbook="trend_pullback", direction=direction, entry_style="pullback_hold", structure=structure, context_profile=ctx, score=score)

    def _early_inflection(
        self,
        frame: Any,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del frame
        plan = self._asset_plan(asset, category)
        canonical = str(asset or "").strip().upper()
        if not (plan.early_inflection_enabled or canonical.endswith("/JPY")):
            return None
        base_direction = _structure_direction(structure, context)
        sign = _playbook_direction_sign(base_direction)
        if sign == 0:
            return None
        exhaustion = _safe_float(structure.get("upside_exhaustion_score" if sign > 0 else "downside_exhaustion_score"), 0.0)
        if exhaustion < 0.60:
            return None
        direction = _direction_label(-sign)
        ctx = self._context_directional_confluence(context, direction)
        score = _clip(_safe_float(structure.get("alignment_score"), 0.0) * 0.25 + _safe_float(structure.get("setup_quality"), 0.0) * 0.22 + exhaustion * 0.30 + ctx["score"] * 0.23, 0.0, 1.0)
        return self._candidate(asset=asset, category=category, session=session, playbook="early_inflection", direction=direction, entry_style="early_inflection_turn", structure=structure, context_profile=ctx, score=score)

    def _reversal_exhaustion(
        self,
        frame: Any,
        *,
        asset: str,
        category: str,
        session: str,
        structure: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        del frame
        base_direction = _structure_direction(structure, context)
        sign = _playbook_direction_sign(base_direction)
        if sign == 0:
            return None
        exhaustion = _safe_float(structure.get("upside_exhaustion_score" if sign > 0 else "downside_exhaustion_score"), 0.0)
        if exhaustion < 0.55 and not bool(structure.get("failed_opposite_move_confirmed")):
            return None
        direction = _direction_label(-sign)
        ctx = self._context_directional_confluence(context, direction)
        score = _clip(_safe_float(structure.get("setup_quality"), 0.0) * 0.25 + exhaustion * 0.38 + ctx["score"] * 0.20 + 0.10, 0.0, 1.0)
        return self._candidate(asset=asset, category=category, session=session, playbook="reversal_exhaustion", direction=direction, entry_style="reclaim_reversal", structure=structure, context_profile=ctx, score=score)

    def analyze(
        self,
        asset: str,
        category: str,
        price_data: Any,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ctx = dict(context) if isinstance(context, dict) else {}
        ctx.setdefault("asset", asset)
        ctx.setdefault("category", str(category or "").strip().lower())
        structure = dict(ctx.get("market_structure") or {})
        plan = self._asset_plan(asset, category)
        profile = self._profile(category)
        allowed, session, allowed_sessions = self._session_allowed(asset, category)
        asset_plan = {
            "playbooks": list(plan.playbooks),
            "preferred_interval": plan.preferred_interval or profile.preferred_interval,
            "allowed_sessions": list(allowed_sessions),
        }
        if not allowed:
            reason = f"session_block:{session}"
            return self._analysis_result(asset, category, session, allowed_sessions, asset_plan, [], reason, [reason], [])

        rejected: List[str] = []
        rejected_details: List[Dict[str, Any]] = []
        candidates: List[Dict[str, Any]] = []
        builders = [
            self._breakout_continuation,
            self._breakout_retest,
            self._trend_pullback,
            self._early_inflection,
            self._reversal_exhaustion,
        ]
        for builder in builders:
            candidate = builder(price_data, asset=asset, category=category, session=session, structure=structure, context=ctx)
            if candidate is None:
                continue
            ok, reason = self._qualify_candidate(candidate, asset=asset, category=category, structure=structure, plan=plan, context=ctx)
            if ok:
                candidates.append(candidate)
            else:
                rejected.append(reason)
                rejected_details.append({"playbook": candidate.get("playbook"), "reason": reason, "score": candidate.get("score"), "confidence": candidate.get("confidence")})

        continuation = self._elite_ready_continuation(asset=asset, category=category, session=session, structure=structure, plan=plan, context=ctx)
        if continuation is not None:
            candidates.append(continuation)

        candidates = sorted(candidates, key=lambda row: (_safe_float(row.get("confidence"), 0.0), _safe_float(row.get("score"), 0.0)), reverse=True)
        blocked_reason = "" if candidates else self._no_seed_reason(asset, category, structure, ctx, rejected_details)
        return self._analysis_result(asset, category, session, allowed_sessions, asset_plan, candidates, blocked_reason, rejected, rejected_details)

    def _analysis_result(
        self,
        asset: str,
        category: str,
        session: str,
        allowed_sessions: Tuple[str, ...],
        asset_plan: Dict[str, Any],
        candidates: List[Dict[str, Any]],
        blocked_reason: str,
        rejected: List[str],
        rejected_details: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        return {
            "asset": asset,
            "category": str(category or "").strip().lower(),
            "session": session,
            "session_label": session,
            "allowed_sessions": list(allowed_sessions),
            "inactivity_profile": {},
            "candidates": candidates,
            "primary": candidates[0] if candidates else None,
            "blocked_reason": blocked_reason,
            "rejected_reasons": rejected,
            "rejected_details": rejected_details,
            "asset_plan": asset_plan,
        }

    def _builder_selection_wait_reason(
        self,
        structure: Dict[str, Any],
        confluence: Dict[str, Any],
        direction: str,
    ) -> str:
        sign = _playbook_direction_sign(direction)
        if sign == 0:
            return "no_direction"

        bias = str(structure.get("structure_bias") or "").strip().lower()
        family = str(structure.get("pattern_family") or "").strip().lower()
        alignment = _safe_float(structure.get("alignment_score"), 0.0)
        setup = _safe_float(structure.get("setup_quality"), 0.0)
        target = _safe_float(structure.get("target_efficiency_score"), 1.0)
        extension = _safe_float(structure.get("extension_score"), 0.0)
        impulse_age = _safe_int(structure.get("impulse_age_bars"), 0)
        cluster = _safe_float(structure.get("cluster_penalty"), 0.0)
        support = int(confluence.get("support_components", 0) or 0)
        depth_override = bool(confluence.get("depth_confirmation_override_allowed"))
        trigger_aligned = bool(structure.get("trigger_trend_aligned")) or _trend_sign(structure.get("trend_5m")) == sign
        trend_aligned = trigger_aligned or _trend_sign(structure.get("trend_15m")) == sign or _trend_sign(structure.get("trend_1h")) == sign
        confirmation_ready = bool(
            structure.get("entry_confirmation_ready")
            or structure.get("fast_entry_confirmation_ready")
            or structure.get("first_pullback_ready")
        )

        if bias == "neutral" and not ("trending_up" in family or "trending_down" in family):
            return "neutral_structure"
        if bias in {"buy", "sell"} and ((bias == "buy" and sign < 0) or (bias == "sell" and sign > 0)):
            return "bias_conflict"
        if alignment < 0.36 or setup < 0.32:
            return "setup_quality_too_weak"
        if cluster > 0.28:
            return "cluster_risk"
        if target < 0.08:
            return "target_space_too_thin"
        if extension > 1.62 and not depth_override:
            return "entry_extended"
        if impulse_age > 16 and not (depth_override and support >= 1):
            return "setup_too_old"
        if not trend_aligned:
            return "trend_misaligned"
        if not confirmation_ready:
            return "confirmation_pending"
        if alignment < 0.48 or setup < 0.46:
            return "builder_threshold_wait"
        if support <= 0:
            return "context_support_missing"
        return "no_builder_selected"

    def _no_seed_reason(
        self,
        asset: str,
        category: str,
        structure: Dict[str, Any],
        context: Dict[str, Any],
        rejected_details: List[Dict[str, Any]],
    ) -> str:
        direction = _structure_direction(structure, context)
        structure_bias = str(structure.get("structure_bias") or "").strip().lower()
        if not direction:
            if structure_bias == "neutral":
                return "depth_context_pressure_wait:neutral_structure"
            micro = dict(context.get("market_microstructure") or {})
            pressure, pressure_ready = _trusted_depth_pressure_score(micro, asset=asset, category=category)
            if pressure_ready:
                return "depth_context_pressure_wait:depth_pressure_weak"
            if bool(micro.get("depth_available")):
                depth = self._depth_readiness(micro, asset=asset, category=category)
                depth_reason = self._depth_wait_reason(depth)
                if depth_reason:
                    return f"depth_context_pressure_wait:{depth_reason}"
            return "depth_context_pressure_wait:no_direction"
        confluence = self._context_directional_confluence(context, direction)
        if rejected_details:
            return _best_rejected_reason(rejected_details)
        builder_reason = self._builder_selection_wait_reason(structure, confluence, direction)
        if builder_reason not in {"no_builder_selected", "context_support_missing"}:
            return f"depth_context_pressure_wait:{builder_reason}"
        if not confluence.get("true_depth_ready") and bool(confluence.get("depth_available")):
            depth_reason = self._depth_wait_reason(confluence)
            if depth_reason:
                return f"depth_context_pressure_wait:{depth_reason}"
        if int(confluence.get("conflict_components", 0) or 0) > 0:
            return "depth_context_pressure_wait:context_conflict"
        if int(confluence.get("support_components", 0) or 0) <= 0 and bool(confluence.get("depth_available")):
            return "depth_context_pressure_wait:context_support_missing"
        if structure_bias == "neutral":
            return "depth_context_pressure_wait:neutral_structure"
        return f"depth_context_pressure_wait:{builder_reason}"

    def pick_seed(
        self,
        asset: str,
        category: str,
        price_data: Any,
        context: Optional[Dict[str, Any]] = None,
        *,
        ml_direction: str = "",
        ml_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        analysis = self.analyze(asset, category, price_data, context=context)
        primary = dict(analysis.get("primary") or {})
        if not primary:
            structure = dict((context or {}).get("market_structure") or {})
            direction = _structure_direction(structure, context)
            context_profile = self._context_directional_confluence(context, direction)
            return {
                "action": "",
                "asset": asset,
                "category": str(category or "").strip().lower(),
                "primary": None,
                "candidates": [],
                "blocked_reason": str(analysis.get("blocked_reason") or "no_playbook_seed"),
                "session": analysis.get("session", ""),
                "session_label": analysis.get("session_label", ""),
                "inactivity_profile": dict(analysis.get("inactivity_profile") or {}),
                "rejected_reasons": list(analysis.get("rejected_reasons") or []),
                "rejected_details": list(analysis.get("rejected_details") or []),
                "allowed_sessions": list(analysis.get("allowed_sessions") or []),
                "asset_plan": dict(analysis.get("asset_plan") or {}),
                "context_direction": direction,
                "context_confluence": round(_safe_float(context_profile.get("score"), 0.0), 4),
                "cross_alignment": round(_safe_float(context_profile.get("cross_support"), 0.0), 4),
                "cross_confidence": round(_safe_float(context_profile.get("cross_confidence"), 0.0), 4),
                "micro_score": round(_safe_float(context_profile.get("micro_support"), 0.0), 4),
                "whale_context_support": round(_safe_float(context_profile.get("whale_support"), 0.0), 4),
                "support_components": int(context_profile.get("support_components", 0) or 0),
                "conflict_components": int(context_profile.get("conflict_components", 0) or 0),
                "score": 0.0,
            }

        ml_dir = str(ml_direction or "").strip().upper()
        ml_conf = _clip(_safe_float(ml_confidence, 0.0), 0.0, 1.0)
        if ml_dir in {"BUY", "SELL"} and ml_dir != primary.get("direction") and ml_conf >= 0.70 and primary.get("confidence", 0.0) < 0.72:
            blocked = f"predictor_conflict:{ml_dir.lower()}"
            return {**analysis, "action": "", "primary": None, "blocked_reason": blocked}

        floor = self._profile(category).seed_floor
        action = "seed" if _safe_float(primary.get("confidence"), 0.0) >= floor else ""
        if not action:
            analysis["blocked_reason"] = "confidence_below_seed_floor"
        return {
            "action": action,
            "asset": asset,
            "category": str(category or "").strip().lower(),
            "session": analysis.get("session", ""),
            "session_label": analysis.get("session_label", ""),
            "primary": primary if action else None,
            "candidates": list(analysis.get("candidates") or []),
            "blocked_reason": "" if action else analysis.get("blocked_reason", ""),
            "rejected_reasons": list(analysis.get("rejected_reasons") or []),
            "rejected_details": list(analysis.get("rejected_details") or []),
            "allowed_sessions": list(analysis.get("allowed_sessions") or []),
            "asset_plan": dict(analysis.get("asset_plan") or {}),
            "inactivity_profile": dict(analysis.get("inactivity_profile") or {}),
        }


_service = PlaybookService()


def get_service() -> PlaybookService:
    return _service
