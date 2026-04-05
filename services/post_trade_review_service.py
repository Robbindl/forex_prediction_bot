from __future__ import annotations

from typing import Any, Dict, List


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_metadata(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    return {}


def _parse_context(raw: Any) -> Dict[str, Any]:
    return raw if isinstance(raw, dict) else {}


def _parse_cross_asset(metadata: Dict[str, Any]) -> Dict[str, Any]:
    raw = metadata.get("cross_asset_context")
    snapshot = dict(raw) if isinstance(raw, dict) else {}
    fallback = {
        "score": metadata.get("cross_asset_score"),
        "alignment": metadata.get("cross_asset_alignment"),
        "confidence": metadata.get("cross_asset_confidence"),
        "state": metadata.get("cross_asset_state"),
        "supportive_direction": metadata.get("cross_asset_supportive_direction"),
        "dominant_peer": metadata.get("cross_asset_primary_peer"),
        "dominant_relation": metadata.get("cross_asset_primary_relation"),
    }
    for key, value in fallback.items():
        if key not in snapshot and value not in (None, "", {}):
            snapshot[key] = value
    return snapshot


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


class PostTradeReviewService:
    def build_review(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        metadata = _parse_metadata(trade.get("metadata") or trade.get("trade_metadata"))
        feedback = metadata.get("execution_feedback")
        if not isinstance(feedback, dict) or not feedback:
            return {}

        memory = metadata.get("setup_memory")
        if not isinstance(memory, dict):
            memory = {}

        structure = metadata.get("market_structure")
        if not isinstance(structure, dict):
            structure = {}

        broker_quality = _parse_context(metadata.get("broker_quality"))
        micro = _parse_context(metadata.get("market_microstructure"))
        cross_asset = _parse_cross_asset(metadata)

        direction = str(trade.get("direction") or trade.get("signal") or feedback.get("direction") or "BUY").upper()
        exit_family = str(feedback.get("exit_family") or "").lower()
        pnl = _safe_float(trade.get("pnl"), 0.0)
        rr_realized = _safe_float(feedback.get("rr_realized"), 0.0)
        target_capture = _safe_float(feedback.get("target_capture"), 0.0)
        giveback_ratio = _safe_float(feedback.get("giveback_ratio"), 0.0)
        execution_drag_rr = _safe_float(feedback.get("execution_drag_rr"), 0.0)
        memory_score = _safe_float(metadata.get("memory_score", memory.get("memory_score")), 0.0)
        memory_edge = _safe_float(metadata.get("memory_edge", memory.get("memory_edge")), 0.0)
        memory_samples = int(_safe_float(metadata.get("memory_sample_count", memory.get("sample_count")), 0))
        opportunity_score = _safe_float(metadata.get("opportunity_score"), 0.0)
        setup_quality = _safe_float(metadata.get("setup_quality"), 0.0)
        alignment_score = _safe_float(metadata.get("alignment_score", structure.get("alignment_score")), 0.0)
        regime = str(feedback.get("regime") or metadata.get("regime") or structure.get("regime") or "unknown").replace("_", " ")
        structure_bias = str(
            feedback.get("structure_bias")
            or metadata.get("structure_bias")
            or structure.get("structure_bias")
            or "neutral"
        ).lower()
        aligned_structure = structure_bias in {"buy", "sell"} and structure_bias == direction.lower()
        broker_context = self._broker_context_state(broker_quality)
        micro_context = self._micro_context_state(micro)
        depth_mode = self._depth_mode(micro)
        cross_asset_context = self._cross_asset_context_state(cross_asset)
        broker_transition_risk = _safe_float(broker_quality.get("market_transition_risk"), 0.0)
        stop_hunt_risk = _safe_float(micro.get("stop_hunt_risk"), 0.0)
        exhaustion_risk = _safe_float(micro.get("exhaustion_risk"), 0.0)
        cross_asset_alignment = _safe_float(cross_asset.get("alignment", cross_asset.get("score")), 0.0)
        cross_asset_confidence = _safe_float(cross_asset.get("confidence"), 0.0)
        cross_asset_peer = str(cross_asset.get("dominant_peer") or "")
        cross_asset_relation = str(cross_asset.get("dominant_relation") or "").replace("_", " ")

        what_went_right: List[str] = []
        what_went_wrong: List[str] = []
        keep: List[str] = []
        avoid: List[str] = []

        target_like = exit_family in {"take_profit", "take_profit_offline", "partial_tp"}
        stop_like = exit_family in {"stop_loss", "stop_loss_offline", "trailing_stop"}
        partial_close = bool(feedback.get("partial_close"))

        outcome = "scratch"
        if target_like or pnl > 0.0 or rr_realized >= 0.25:
            outcome = "win"
        elif stop_like or pnl < 0.0 or rr_realized <= -0.25:
            outcome = "loss"
        if partial_close and pnl > 0.0:
            outcome = "partial_win"

        if outcome in {"win", "partial_win"}:
            if target_like and target_capture >= 0.95:
                what_went_right.append("The move followed through cleanly enough to pay the planned target.")
                keep.append("Keep letting clean trend-following moves reach their planned objective.")
            elif pnl > 0.0:
                what_went_right.append("The trade direction was right and the exit still locked in a profit.")
                keep.append("Keep pressing valid direction calls even when the move does not extend all the way.")

            if giveback_ratio <= 0.25 and target_capture >= 0.70:
                what_went_right.append("Profit capture was efficient, with limited giveback after the trade moved your way.")
                keep.append("Keep protecting winners once they have already delivered meaningful progress.")

            if aligned_structure and (alignment_score >= 0.55 or setup_quality >= 0.55):
                what_went_right.append(f"Structure stayed aligned with the {direction} idea in a {regime} backdrop.")
                keep.append("Keep prioritizing setups where structure and regime point in the same direction.")

            if memory_samples >= 6 and (memory_edge >= 0.18 or memory_score >= 62.0):
                what_went_right.append("This resembled a setup family that already had positive memory behind it.")
                keep.append("Keep trusting repeated setups that show a real positive historical edge.")

            if opportunity_score >= 0.75:
                what_went_right.append("The opportunity quality was strong enough to justify staying with the trade plan.")
                keep.append("Keep sizing up only when the full opportunity picture is genuinely strong.")

            if execution_drag_rr > 0.08:
                what_went_right.append("The trade still worked even after realistic execution drag took a measurable bite out of the result.")
                keep.append("Keep favoring the cleaner setups where the edge is large enough to survive execution costs.")

            if broker_context == "supportive":
                what_went_right.append("Broker quotes stayed aligned and usable enough to confirm the setup instead of fighting it.")
                keep.append("Keep trusting trades more when brokers agree and quote quality stays stable.")

            if micro_context == "supportive":
                if depth_mode == "true_depth":
                    what_went_right.append("Real depth and short-horizon flow stayed supportive into the move.")
                    keep.append("Keep leaning harder on setups backed by true depth and aligned flow.")
                else:
                    what_went_right.append("Top-of-book pressure stayed supportive enough to keep the trade onside.")
                    keep.append("Keep respecting supportive quote pressure when it stays aligned through the entry.")
            if cross_asset_context == "supportive" and cross_asset_peer:
                relation_text = f" through {cross_asset_relation}" if cross_asset_relation else ""
                what_went_right.insert(
                    0,
                    f"Related-market confirmation from {cross_asset_peer}{relation_text} stayed aligned with the trade."
                )
                keep.insert(0, "Keep trusting spillover more when the main peer is confirming in the same direction.")

        else:
            if bool(feedback.get("late_entry")):
                what_went_wrong.append("The entry arrived late, so the trade took heat before it had enough room to work.")
                avoid.append("Avoid chasing entries after the move is already mature.")

            if bool(feedback.get("premature_stop")):
                what_went_wrong.append("The trade showed early progress, then gave it back before protection tightened.")
                avoid.append("Avoid letting early unrealized progress round-trip back into the stop.")

            if bool(feedback.get("stop_too_tight")):
                what_went_wrong.append("The stop appears to have been too tight for the amount of normal market noise.")
                avoid.append("Avoid cramped stops when volatility is still noisy around entry.")

            if bool(feedback.get("stop_too_wide")):
                what_went_wrong.append("The trade never built enough favorable progress, so the stop gave too much room to a weak idea.")
                avoid.append("Avoid giving full-size stop room to setups that do not show early proof quickly enough.")

            if bool(feedback.get("target_miss")):
                what_went_wrong.append("Price got most of the way toward the target, but the trade did not capture enough of that move.")
                avoid.append("Avoid waiting passively when price has already done most of the heavy lifting.")

            if memory_samples >= 6 and (memory_edge <= -0.12 or memory_score <= 42.0):
                what_went_wrong.append("Similar setups already had weak historical memory, so this was not a high-quality pattern to force.")
                avoid.append("Avoid forcing patterns that already show a negative live memory edge.")

            if execution_drag_rr > 0.08:
                what_went_wrong.append("Execution drag was meaningful, so the trade needed more edge than it actually had.")
                avoid.append("Avoid marginal setups when spread, slippage, and fees are already taking a noticeable share of the risk.")

            if broker_context == "fragile":
                what_went_wrong.append("Broker confirmation was weak, with disagreement, stale quotes, or stressed spreads reducing entry quality.")
                avoid.append("Avoid forcing trades when brokers disagree or quote quality is already degraded.")

            if broker_transition_risk >= 0.65:
                what_went_wrong.append("The market was transitioning between states, which made the trade less trustworthy at entry.")
                avoid.append("Avoid marginal entries while the market state is still flipping or unstable.")

            if micro_context == "hostile":
                if depth_mode == "synthetic_depth":
                    what_went_wrong.append("Only proxy depth was available and the microstructure was hostile, so the entry had thinner proof than it looked.")
                    avoid.append("Avoid leaning on synthetic depth alone when stop-hunt or exhaustion risk is already elevated.")
                else:
                    what_went_wrong.append("Short-horizon flow was hostile enough to work against the entry soon after it opened.")
                    avoid.append("Avoid entries when live flow shows stop-hunt pressure or exhaustion against the trade.")
            elif stop_hunt_risk >= 0.45:
                what_went_wrong.append("Stop-hunt risk was elevated, so the setup needed cleaner confirmation than it had.")
                avoid.append("Avoid entries into obvious sweep conditions unless the broader edge is exceptional.")
            elif exhaustion_risk >= 0.42:
                what_went_wrong.append("The move was already showing exhaustion, so the trade did not have fresh enough energy behind it.")
                avoid.append("Avoid late entries into moves that are already tiring at the quote-flow level.")
            if cross_asset_context == "conflicted" and cross_asset_peer:
                relation_text = f" through {cross_asset_relation}" if cross_asset_relation else ""
                what_went_wrong.insert(
                    0,
                    f"Related-market spillover from {cross_asset_peer}{relation_text} was leaning against the trade at entry."
                )
                avoid.insert(0, "Avoid forcing trades when the dominant related market is pointing the other way.")

            if not what_went_wrong:
                what_went_wrong.append("The setup failed to generate enough follow-through after entry.")
                avoid.append("Avoid repeating the same setup without stronger confirmation.")

        lesson = self._derive_lesson(
            outcome=outcome,
            feedback=feedback,
            aligned_structure=aligned_structure,
            memory_score=memory_score,
            memory_edge=memory_edge,
            memory_samples=memory_samples,
            broker_context=broker_context,
            micro_context=micro_context,
            depth_mode=depth_mode,
            cross_asset_context=cross_asset_context,
            cross_asset_peer=cross_asset_peer,
            cross_asset_relation=cross_asset_relation,
            broker_quality=broker_quality,
            micro=micro,
        )
        next_focus = self._derive_next_focus(
            outcome=outcome,
            keep=keep,
            avoid=avoid,
        )

        if not what_went_right and outcome in {"win", "partial_win"}:
            what_went_right.append("The trade plan was broadly correct and the market paid it.")
        if not keep and outcome in {"win", "partial_win"}:
            keep.append("Keep repeating the parts of the setup that stayed aligned from entry to exit.")

        headline = self._headline(outcome, exit_family)
        summary = self._summary(
            outcome=outcome,
            direction=direction,
            what_went_right=what_went_right,
            what_went_wrong=what_went_wrong,
            lesson=lesson,
        )

        return {
            "version": 1,
            "outcome": outcome,
            "headline": headline,
            "summary": summary,
            "lesson": lesson,
            "next_focus": next_focus,
            "what_went_right": what_went_right[:3],
            "what_went_wrong": what_went_wrong[:3],
            "keep": keep[:3],
            "avoid": avoid[:3],
            "quality_score": round(_clip(_safe_float(feedback.get("quality_score"), 50.0), 0.0, 100.0), 1),
            "rr_realized": round(rr_realized, 4),
            "target_capture": round(target_capture, 4),
            "memory_score": round(memory_score, 1),
            "memory_edge": round(memory_edge, 4),
            "memory_sample_count": memory_samples,
            "entry_diagnostics": {
                "broker_context": broker_context,
                "micro_context": micro_context,
                "depth_mode": depth_mode,
                "primary_provider": str(broker_quality.get("primary_provider") or ""),
                "quote_agreement_state": str(broker_quality.get("quote_agreement_state") or ""),
                "spread_regime": str(broker_quality.get("spread_regime") or ""),
                "quote_quality_state": str(broker_quality.get("quote_quality_state") or ""),
                "market_transition_risk": round(broker_transition_risk, 4),
                "stop_hunt_risk": round(stop_hunt_risk, 4),
                "exhaustion_risk": round(exhaustion_risk, 4),
                "cross_asset_context": cross_asset_context,
                "cross_asset_alignment": round(cross_asset_alignment, 4),
                "cross_asset_confidence": round(cross_asset_confidence, 4),
                "cross_asset_primary_peer": cross_asset_peer,
                "cross_asset_primary_relation": str(cross_asset.get("dominant_relation") or ""),
            },
        }

    @staticmethod
    def _headline(outcome: str, exit_family: str) -> str:
        if outcome == "partial_win":
            return "The trade paid something and gave useful continuation feedback."
        if outcome == "win":
            if "take_profit" in exit_family:
                return "The trade plan was right and the market completed enough of the move."
            return "The position finished on the right side of the move."
        if outcome == "loss":
            if "stop_loss" in exit_family:
                return "The trade failed before the setup could prove itself."
            return "The trade did not produce enough follow-through."
        return "The result was mixed and the execution needs context."

    @staticmethod
    def _summary(
        outcome: str,
        direction: str,
        what_went_right: List[str],
        what_went_wrong: List[str],
        lesson: str,
    ) -> str:
        if outcome in {"win", "partial_win"}:
            primary = what_went_right[0] if what_went_right else f"The {direction} idea worked."
            return f"{primary} Lesson kept: {lesson}"
        primary = what_went_wrong[0] if what_went_wrong else f"The {direction} idea did not work."
        return f"{primary} Main lesson: {lesson}"

    @staticmethod
    def _derive_lesson(
        outcome: str,
        feedback: Dict[str, Any],
        aligned_structure: bool,
        memory_score: float,
        memory_edge: float,
        memory_samples: int,
        broker_context: str,
        micro_context: str,
        depth_mode: str,
        cross_asset_context: str,
        cross_asset_peer: str,
        cross_asset_relation: str,
        broker_quality: Dict[str, Any],
        micro: Dict[str, Any],
    ) -> str:
        if outcome in {"win", "partial_win"}:
            if broker_context == "supportive" and micro_context == "supportive" and depth_mode == "true_depth":
                return "When broker confirmation and true depth both support the trade, the bot can trust the original plan more."
            if broker_context == "supportive":
                return "When brokers stay aligned and quote quality holds up, the bot should trust the setup more."
            if cross_asset_context == "supportive" and cross_asset_peer:
                relation_text = f" through {cross_asset_relation}" if cross_asset_relation else ""
                return f"When {cross_asset_peer} confirms the trade{relation_text}, the bot can trust the setup more."
            if aligned_structure:
                return "When structure, regime, and direction stay aligned, the bot should keep respecting the original plan."
            if memory_samples >= 6 and (memory_edge >= 0.18 or memory_score >= 62.0):
                return "Positive setup memory deserves continued trust when the sample size is real."
            return "The bot should keep favoring trades that move cleanly soon after entry and protect gains without panic."

        if broker_context == "fragile":
            return "When brokers disagree, quotes are stale, or spreads are stressed, the setup should clear a higher bar before entry."
        if cross_asset_context == "conflicted" and cross_asset_peer:
            relation_text = f" through {cross_asset_relation}" if cross_asset_relation else ""
            return f"When {cross_asset_peer} is leaning the other way{relation_text}, the setup should clear a higher bar before entry."
        if micro_context == "hostile" and depth_mode == "synthetic_depth":
            return "When only proxy depth is available and the microstructure is hostile, the bot should wait for cleaner flow."
        if _safe_float(micro.get("stop_hunt_risk"), 0.0) >= 0.45:
            return "Avoid entries into stop-hunt conditions unless the broader edge is overwhelming."
        if _safe_float(broker_quality.get("market_transition_risk"), 0.0) >= 0.65:
            return "Do not force trades while the market is still transitioning between stable trading states."
        if bool(feedback.get("late_entry")):
            return "Do not chase extended entries; wait for fresher structure or better price location."
        if bool(feedback.get("premature_stop")):
            return "Once a trade has already produced meaningful favorable movement, protection should tighten sooner."
        if bool(feedback.get("stop_too_tight")):
            return "Stops should reflect actual volatility instead of being squeezed too close to entry."
        if bool(feedback.get("stop_too_wide")):
            return "Weak setups that fail to show progress quickly should not be given full breathing room."
        if bool(feedback.get("target_miss")):
            return "When price has already reached most of the objective, the bot should secure more of the move."
        if memory_samples >= 6 and (memory_edge <= -0.12 or memory_score <= 42.0):
            return "Negative setup memory should carry more weight before approving similar trades again."
        return "The setup needed stronger confirmation before entry and better evidence after entry."

    @staticmethod
    def _derive_next_focus(outcome: str, keep: List[str], avoid: List[str]) -> str:
        if outcome in {"win", "partial_win"}:
            return keep[0] if keep else "Keep repeating the parts of the setup that stayed disciplined."
        return avoid[0] if avoid else "Avoid repeating the same weak pattern without stronger confirmation."

    @staticmethod
    def _depth_mode(micro: Dict[str, Any]) -> str:
        if bool(micro.get("depth_available")):
            return "true_depth"
        if bool(micro.get("synthetic_depth_available")):
            return "synthetic_depth"
        return "top_of_book"

    @staticmethod
    def _broker_context_state(broker_quality: Dict[str, Any]) -> str:
        agreement_state = str(broker_quality.get("quote_agreement_state") or "").lower()
        spread_regime = str(broker_quality.get("spread_regime") or "").lower()
        quote_quality_state = str(broker_quality.get("quote_quality_state") or "").lower()
        score = _safe_float(broker_quality.get("score"), 0.0)
        transition_risk = _safe_float(broker_quality.get("market_transition_risk"), 0.0)
        if (
            agreement_state in {"divergent", "severe_divergence"}
            or spread_regime in {"stressed", "extreme"}
            or quote_quality_state in {"stale", "delayed"}
            or transition_risk >= 0.65
            or bool(broker_quality.get("market_state_changed"))
        ):
            return "fragile"
        if (
            score >= 0.65
            and agreement_state in {"strong", "aligned"}
            and spread_regime in {"tight", "normal"}
            and quote_quality_state in {"fresh", "aging"}
        ):
            return "supportive"
        return "mixed"

    @staticmethod
    def _micro_context_state(micro: Dict[str, Any]) -> str:
        aligned_micro = _safe_float(
            micro.get("microstructure_alignment", micro.get("score")),
            0.0,
        )
        stop_hunt_risk = _safe_float(micro.get("stop_hunt_risk"), 0.0)
        exhaustion_risk = _safe_float(micro.get("exhaustion_risk"), 0.0)
        if stop_hunt_risk >= 0.45 or exhaustion_risk >= 0.42 or aligned_micro <= -0.20:
            return "hostile"
        if aligned_micro >= 0.20 and stop_hunt_risk <= 0.20 and exhaustion_risk <= 0.20:
            return "supportive"
        return "mixed"

    @staticmethod
    def _cross_asset_context_state(cross_asset: Dict[str, Any]) -> str:
        aligned = _safe_float(cross_asset.get("alignment", cross_asset.get("score")), 0.0)
        confidence = _safe_float(cross_asset.get("confidence"), 0.0)
        if confidence < 0.20:
            return "mixed"
        if aligned >= 0.20:
            return "supportive"
        if aligned <= -0.20:
            return "conflicted"
        return "mixed"


_service = PostTradeReviewService()


def get_service() -> PostTradeReviewService:
    return _service
