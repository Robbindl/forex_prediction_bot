"""Layer 5 — Sentiment filter. Rewritten from sentiment_analyzer.py."""
from __future__ import annotations
import threading
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 5

# Sentiment score ranges: -1.0 (bearish) → +1.0 (bullish)
_STRONG_THRESHOLD  = 0.4
_WEAK_THRESHOLD    = -0.3
_KILL_THRESHOLD    = -0.6

# Module-level singleton — initialised once, reused across all signals (Issue 5)
_sa_instance = None
_sa_lock     = threading.Lock()


def _get_analyzer():
    """Return the shared SentimentAnalyzer, creating it on first call."""
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
    """
    Fetch sentiment score. Returns 0.0 (neutral) if unavailable.
    Uses the module-level singleton — no re-initialisation per signal.
    """
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


class SentimentLayer:
    name = "sentiment"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        score = context.get("sentiment_score")
        if score is None:
            score = _fetch_sentiment(signal.asset, signal.category)

        signal.metadata["sentiment_score"] = round(score, 3)

        direction_sign = 1 if signal.direction == "BUY" else -1
        aligned_score  = score * direction_sign

        if aligned_score <= _KILL_THRESHOLD:
            signal.kill(f"Sentiment strongly against {signal.direction} (score={score:.3f})", LAYER)
            return None

        if aligned_score >= _STRONG_THRESHOLD:
            signal.boost(0.04)
        elif aligned_score <= _WEAK_THRESHOLD:
            signal.reduce(0.03)

        logger.log_pipeline(signal.asset, LAYER, "PASS", f"sentiment={score:.3f}")
        return signal