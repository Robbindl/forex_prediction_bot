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
    ) -> str:
        if outcome in {"win", "partial_win"}:
            if aligned_structure:
                return "When structure, regime, and direction stay aligned, the bot should keep respecting the original plan."
            if memory_samples >= 6 and (memory_edge >= 0.18 or memory_score >= 62.0):
                return "Positive setup memory deserves continued trust when the sample size is real."
            return "The bot should keep favoring trades that move cleanly soon after entry and protect gains without panic."

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


_service = PostTradeReviewService()


def get_service() -> PostTradeReviewService:
    return _service
