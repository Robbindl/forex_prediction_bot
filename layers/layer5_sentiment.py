"""Layer 5 — Sentiment filter. Rewritten from sentiment_analyzer.py."""
from __future__ import annotations
from typing import Any, Dict, Optional
from core.signal import Signal
from utils.logger import get_logger

logger = get_logger()
LAYER = 5

# Sentiment score ranges: -1.0 (bearish) → +1.0 (bullish)
_STRONG_THRESHOLD  = 0.4
_WEAK_THRESHOLD    = -0.3
_KILL_THRESHOLD    = -0.6


def _fetch_sentiment(asset: str, category: str) -> float:
    """
    Fetch sentiment score. Returns 0.0 (neutral) if unavailable.
    Real implementation wires to news_sources.py + textblob.
    """
    try:
        from sentiment_analyzer import SentimentAnalyzer
        sa    = SentimentAnalyzer()
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