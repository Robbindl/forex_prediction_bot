from __future__ import annotations

from typing import Any, Dict, Mapping, Optional


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


class AdaptivePolicyService:
    def get_thresholds(
        self,
        asset: str,
        category: str,
        context: Optional[Mapping[str, Any]] = None,
        signal: Any | None = None,
        state: Any | None = None,
        **_: Any,
    ) -> Dict[str, Any]:
        context = dict(context or {})
        metadata = dict(getattr(signal, "metadata", {}) or {})
        structure = dict(context.get("market_structure") or metadata.get("market_structure") or {})
        micro = dict(context.get("market_microstructure") or metadata.get("market_microstructure") or {})

        alignment = _safe_float(structure.get("alignment_score", metadata.get("alignment_score")), 0.0)
        setup = _safe_float(structure.get("setup_quality", metadata.get("setup_quality")), 0.0)
        extension = _safe_float(structure.get("extension_score", metadata.get("extension_score")), 0.0)
        target = _safe_float(structure.get("target_efficiency_score", metadata.get("target_efficiency_score")), 0.0)
        depth_quality = _safe_float(micro.get("depth_quality", metadata.get("depth_quality")), 0.0)
        depth_trust = _safe_float(micro.get("depth_provider_trust_score", metadata.get("depth_provider_trust_score")), 0.0)
        depth_available = bool(micro.get("depth_available") or metadata.get("depth_available"))

        category_key = str(category or "").strip().lower()
        max_spread = {
            "forex": 0.0025,
            "indices": 0.0045,
            "commodities": 0.0040,
            "crypto": 0.0035,
        }.get(category_key, 0.0035)
        max_spread_bps = {
            "forex": 12.0,
            "indices": 24.0,
            "commodities": 22.0,
            "crypto": 18.0,
        }.get(category_key, 18.0)

        min_conf = 0.58
        min_rr = 1.45
        risk_multiplier = 1.0
        notes: list[str] = []

        if alignment >= 0.60 and setup >= 0.50:
            min_conf -= 0.03
            risk_multiplier += 0.08
            notes.append("structure_support")
        if depth_available and depth_quality >= 0.55 and depth_trust >= 0.58:
            min_conf -= 0.025
            risk_multiplier += 0.05
            notes.append("true_depth_support")
        if extension > 1.12:
            min_conf += min(0.06, (extension - 1.12) * 0.10)
            risk_multiplier -= 0.08
            notes.append("extension_risk")
        if target < 0.16:
            min_rr += 0.15
            notes.append("target_efficiency_low")

        thresholds = {
            "asset": asset,
            "category": category_key,
            "min_final_confidence": round(_clip(min_conf, 0.52, 0.70), 4),
            "min_confidence": round(_clip(min_conf, 0.52, 0.70), 4),
            "max_spread": round(max_spread, 6),
            "max_spread_bps": round(max_spread_bps, 4),
            "risk_multiplier": round(_clip(risk_multiplier, 0.65, 1.25), 4),
            "cooldown_minutes": 12,
            "min_rr": round(_clip(min_rr, 1.20, 2.20), 2),
            "target_rr_multiplier": 1.0,
            "block_new_entries": False,
            "block_reason": "",
            "notes": notes,
            "recent_review_profile": {"sample_count": 0},
            "asset_performance_profile": {},
            "book_performance_profile": {},
            "context_protection_profile": {"action": "none", "sample_count": 0},
            "session_performance_profile": {},
            "inactivity_profile": {},
        }
        try:
            from services.execution_feedback_service import get_service as get_execution_feedback_service

            feedback = get_execution_feedback_service()
            profile = dict(
                feedback.summarize_context(
                    asset=asset,
                    category=category_key,
                    playbook_name=metadata.get("playbook_name", ""),
                    session=metadata.get("session_label", ""),
                )
                or {}
            )
        except Exception:
            profile = {"sample_count": 0}
        sample_count = int(_safe_float(profile.get("sample_count"), 0.0))
        if sample_count >= 5:
            profit_factor = _safe_float(profile.get("profit_factor"), 1.0)
            avg_rr = _safe_float(profile.get("avg_rr_realized"), 0.0)
            if profit_factor < 0.65 or avg_rr < -0.20:
                thresholds["context_protection_profile"] = dict(profile, action="reduce")
                thresholds["risk_multiplier"] = round(_clip(float(thresholds["risk_multiplier"]) - 0.12, 0.65, 1.25), 4)
                thresholds["cooldown_minutes"] = max(int(thresholds["cooldown_minutes"]), 14)
                thresholds["notes"].append("playbook_session_protection_reduce")
            if profit_factor < 0.45 or avg_rr < -0.30:
                thresholds["context_protection_profile"] = dict(profile, action="reduce")
                thresholds["block_new_entries"] = True
                thresholds["block_reason"] = (
                    f"protection lock: {metadata.get('playbook_name', '')} "
                    f"in {metadata.get('session_label', '')} is underperforming for {category_key}"
                )
                thresholds["notes"].append("playbook_session_protection_lock")
        thresholds["raw"] = dict(thresholds)
        return thresholds


_service = AdaptivePolicyService()


def get_service() -> AdaptivePolicyService:
    return _service
