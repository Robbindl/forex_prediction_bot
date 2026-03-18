from __future__ import annotations
import threading
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS, KILLED
from utils.logger import get_logger

logger = get_logger()
LAYER = 5

_STRONG_THRESHOLD = 0.4
_WEAK_THRESHOLD   = -0.3
_KILL_THRESHOLD   = -0.6

_sa_instance = None
_sa_lock     = threading.Lock()


def _get_analyzer():
    global _sa_instance
    if _sa_instance is not None:
        return _sa_instance
    with _sa_lock:
        if _sa_instance is None:
            try:
                from sentiment_analyzer import SentimentAnalyzer
                _sa_instance = SentimentAnalyzer()
                logger.info("[SentimentLayer] SentimentAnalyzer initialised (singleton)")
            except Exception as e:
                logger.warning(f"[SentimentLayer] Analyzer init failed: {e}")
                _sa_instance = None
    return _sa_instance


def _fetch_sentiment(asset: str, category: str) -> float:
    try:
        sa = _get_analyzer()
        if sa is None:
            return 0.0
        result = sa.get_comprehensive_sentiment(asset, category)
        if isinstance(result, dict):
            return float(result.get("composite_score", 0.0))
        return float(result)
    except Exception:
        return 0.0


def _get_narrative_data() -> Dict[str, Any]:
    """
    Pull narrative scores from Phase 4 narrative_ai.
    Returns dominant narrative and top score.
    """
    try:
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        scores   = get_narrative_scores()
        dominant = get_dominant_narrative()
        top_score = max(scores.values()) if scores else 0.0
        return {
            "dominant_narrative": dominant,
            "narrative_strength": round(top_score, 3),
            "phase4": "narrative_ai",
        }
    except Exception:
        return {"phase4": "unavailable"}


class SentimentLayer:
    name = "sentiment"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence

        # ── Sentiment score ───────────────────────────────────────────────
        score = context.get("sentiment_score")
        if score is None:
            score = _fetch_sentiment(signal.asset, signal.category)
        signal.metadata["sentiment_score"] = round(score, 3)

        # ── Phase 4: Narrative data ───────────────────────────────────────
        narrative_data = _get_narrative_data()
        dominant       = narrative_data.get("dominant_narrative", "")
        nar_strength   = narrative_data.get("narrative_strength", 0.0)

        direction_sign = 1 if signal.direction == "BUY" else -1
        aligned_score  = score * direction_sign

        # ── Kill on strongly opposing sentiment ───────────────────────────
        if aligned_score <= _KILL_THRESHOLD:
            reason = f"sentiment strongly against {signal.direction} (score={score:.3f})"
            signal.reduce(0.12)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={
                    "sentiment_score": round(score, 3),
                    **narrative_data,
                },
            )
            logger.log_pipeline(signal.asset, LAYER, "STRONG_OPPOSE", reason)

        # ── Adjustments ───────────────────────────────────────────────────
        if aligned_score >= _STRONG_THRESHOLD:
            signal.boost(0.04)
        elif aligned_score <= _WEAK_THRESHOLD:
            signal.reduce(0.03)

        # Narrative momentum boost — strong narrative + aligned direction
        if nar_strength > 0.10 and dominant:
            bullish_narratives = {"ETF_NEWS", "HALVING_BUZZ", "AI_TOKENS", "LAYER2_TREND"}
            bearish_narratives = {"REGULATION", "MACRO_SHOCK", "EXCHANGE_NEWS", "STABLECOIN_NEWS"}
            if (signal.direction == "BUY"  and dominant in bullish_narratives) or \
               (signal.direction == "SELL" and dominant in bearish_narratives):
                signal.boost(0.02)
                narrative_data["narrative_boost"] = "+0.02"

        reason = (
            f"sentiment={score:+.3f}  "
            f"narrative={dominant or 'none'}({nar_strength:.2f})"
        )
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "sentiment_score": round(score, 3),
                **narrative_data,
            },
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS",
                            f"sentiment={score:.3f} narrative={dominant}")
        return signal