"""
layers/layer5_sentiment.py — Asset-aware sentiment layer.

Changes vs original:
  - Consult AssetProfile before applying ANY sentiment source
  - AAII is only applied to US indices (^DJI, ^IXIC, ^GSPC)
  - Put/call ratio is only applied to US indices
  - Reddit sentiment is only applied to crypto
  - Silent failures replaced with explicit None + logging
  - Narrative AI empty-result warning added
  - Data source tracking written to signal.metadata for pipeline gating
"""
from __future__ import annotations

import threading
from typing import Any, Dict, Optional

from core.signal import Signal
from core.signal_journal import PASS, KILLED
from core.asset_profiles import get_profile, is_us_index, is_crypto
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
                logger.error(f"[SentimentLayer] Analyzer init failed: {e}")
                _sa_instance = None
    return _sa_instance


def _fetch_sentiment(asset: str, category: str) -> Optional[float]:
    """
    Fetch composite sentiment score.
    Returns None if the analyser is unavailable — never returns a fake default.
    """
    try:
        sa = _get_analyzer()
        if sa is None:
            logger.warning(f"[SentimentLayer] SentimentAnalyzer unavailable for {asset}")
            return None
        result = sa.get_comprehensive_sentiment(asset)
        if isinstance(result, dict):
            score = result.get("composite_score")
            if score is None:
                logger.warning(f"[SentimentLayer] Missing composite_score in result for {asset}")
                return None
            return float(score)
        return float(result)
    except Exception as e:
        logger.error(f"[SentimentLayer] Fetch failed for {asset}: {e}")
        return None


def _fetch_aaii(asset: str) -> Optional[float]:
    """
    Fetch AAII bull/bear spread as a -1..+1 score.
    ONLY valid for US indices.  Returns None with a warning for all other assets.
    """
    if not is_us_index(asset):
        logger.debug(f"[SentimentLayer] AAII skipped — {asset} is not a US index")
        return None
    try:
        sa = _get_analyzer()
        if sa is None:
            return None
        raw = sa.fetch_aaii_sentiment()
        if not raw or not isinstance(raw, dict):
            logger.warning(f"[SentimentLayer] AAII returned invalid data for {asset}: {raw!r}")
            return None
        # Validate required fields
        required = {"bullish", "bearish", "neutral"}
        missing = required - set(raw.keys())
        if missing:
            logger.error(f"[SentimentLayer] AAII missing fields {missing} for {asset}")
            return None
        bull = float(raw["bullish"])
        bear = float(raw["bearish"])
        # Bull-bear spread normalised to -1..+1
        spread = (bull - bear) / 100.0
        logger.debug(f"[SentimentLayer] AAII {asset}: bull={bull:.1f} bear={bear:.1f} spread={spread:+.3f}")
        return spread
    except Exception as e:
        logger.error(f"[SentimentLayer] AAII fetch failed for {asset}: {e}")
        return None


def _fetch_put_call(asset: str) -> Optional[float]:
    """
    Fetch equity put/call ratio and convert to a -1..+1 directional score.
    ONLY valid for US indices.  Returns None for all other assets.

    Scoring:
        ratio > 1.0  → bearish  (score  = -(ratio - 1) / ratio)
        ratio < 1.0  → bullish  (score  =  (1 - ratio))
        ratio == 1.0 → neutral  (score  =  0.0)
    """
    if not is_us_index(asset):
        logger.debug(f"[SentimentLayer] Put/call skipped — {asset} is not a US index")
        return None
    try:
        import os, requests
        api_key = os.getenv("FMP_API_KEY", "")
        if not api_key or "your_" in api_key:
            logger.warning("[SentimentLayer] FMP_API_KEY not configured — put/call unavailable")
            return None
        url = (
            f"https://financialmodelingprep.com/api/v4/put_call_ratio"
            f"?apikey={api_key}"
        )
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        if not data or not isinstance(data, list):
            logger.warning(f"[SentimentLayer] Put/call API returned empty data for {asset}")
            return None
        ratio = float(data[0].get("putCallRatio", 1.0))
        if ratio > 1.0:
            score = -min(1.0, (ratio - 1.0) / ratio)
        elif ratio < 1.0:
            score = min(1.0, 1.0 - ratio)
        else:
            score = 0.0
        logger.debug(f"[SentimentLayer] Put/call {asset}: ratio={ratio:.3f} score={score:+.3f}")
        return score
    except Exception:
        pass
    return None

def _fetch_reddit_sentiment(asset: str) -> Optional[float]:
    if not is_crypto(asset):
        return None
    try:
        from sentiment_analyzer import SentimentAnalyzer
        sa = SentimentAnalyzer()
        if not sa.reddit or not sa.reddit.enabled:
            return None    
        score = result.get("score")
        if score is None:
            logger.warning(f"[SentimentLayer] Reddit missing score field for {asset}")
            return None
        return float(score)
    except Exception as e:
        logger.error(f"[SentimentLayer] Reddit sentiment failed for {asset}: {e}")
        return None


def _get_narrative_data(asset: str) -> Dict[str, Any]:
    """
    Pull narrative scores from Phase 4 narrative_ai.
    Returns empty dict (NOT fake data) if unavailable.
    """
    try:
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        scores   = get_narrative_scores()
        dominant = get_dominant_narrative()
        if not scores:
            logger.debug(f"[SentimentLayer] Narrative AI returned empty scores for {asset}")
            return {"phase4": "empty"}
        top_score = max(scores.values())
        return {
            "dominant_narrative": dominant,
            "narrative_strength": round(top_score, 3),
            "phase4": "narrative_ai",
        }
    except Exception as e:
        logger.error(f"[SentimentLayer] Narrative AI failed for {asset}: {e}")
        return {"phase4": "unavailable"}


class SentimentLayer:
    name = "sentiment"

    def process(self, signal: Signal, context: Dict[str, Any]) -> Optional[Signal]:
        conf_before = signal.confidence
        profile     = get_profile(signal.asset)

        # ── Track which sources actually provided data ─────────────────────
        sources_valid   = 0
        sources_applied = []
        sources_skipped = []

        # ── 1. Composite news/social sentiment ────────────────────────────
        score = context.get("sentiment_score")
        if score is None:
            score = _fetch_sentiment(signal.asset, signal.category)

        if score is None:
            # No sentiment data available — proceed without adjusting
            sources_skipped.append("composite_sentiment")
        else:
            signal.metadata["sentiment_score"] = round(score, 3)
            sources_valid += 1
            sources_applied.append("composite_sentiment")

        # ── 2. AAII (US indices only) ──────────────────────────────────────
        aaii_score: Optional[float] = None
        if profile.use_aaii:
            aaii_score = _fetch_aaii(signal.asset)
            if aaii_score is None:
                sources_skipped.append("aaii")
            else:
                signal.metadata["aaii_score"] = round(aaii_score, 3)
                sources_valid += 1
                sources_applied.append("aaii")
        else:
            sources_skipped.append("aaii_not_applicable")

        # ── 3. Put/call ratio (US indices only) ───────────────────────────
        pc_score: Optional[float] = None
        if profile.use_put_call:
            pc_score = _fetch_put_call(signal.asset)
            if pc_score is None:
                sources_skipped.append("put_call")
            else:
                signal.metadata["put_call_score"] = round(pc_score, 3)
                sources_valid += 1
                sources_applied.append("put_call")
        else:
            sources_skipped.append("put_call_not_applicable")

        # ── 4. Reddit (crypto only) ────────────────────────────────────────
        reddit_score: Optional[float] = None
        if profile.use_reddit:
            reddit_score = _fetch_reddit_sentiment(signal.asset)
            if reddit_score is None:
                sources_skipped.append("reddit")
            else:
                signal.metadata["reddit_score"] = round(reddit_score, 3)
                sources_valid += 1
                sources_applied.append("reddit")
        else:
            sources_skipped.append("reddit_not_applicable")

        # ── 5. Phase 4 Narrative AI ────────────────────────────────────────
        narrative_data = _get_narrative_data(signal.asset)
        dominant       = narrative_data.get("dominant_narrative", "")
        nar_strength   = narrative_data.get("narrative_strength", 0.0)

        # ── Composite score: weighted average of available sources ────────
        available_scores = []
        weights          = []

        if score is not None:
            available_scores.append(score)
            weights.append(0.4)

        if aaii_score is not None:
            available_scores.append(aaii_score)
            weights.append(0.25)

        if pc_score is not None:
            available_scores.append(pc_score)
            weights.append(0.2)

        if reddit_score is not None:
            available_scores.append(reddit_score)
            weights.append(0.15)

        if available_scores:
            total_w       = sum(weights[:len(available_scores)])
            composite     = sum(s * w for s, w in zip(available_scores, weights)) / total_w
        else:
            # Zero real sentiment sources — pass neutral, record it
            composite = 0.0
            signal.metadata["sentiment_warning"] = "no_sources_available"
            logger.warning(f"[SentimentLayer] No sentiment sources available for {signal.asset}")

        signal.metadata["sentiment_composite"] = round(composite, 3)
        signal.metadata["sentiment_sources"]   = sources_applied
        signal.metadata["sentiment_skipped"]   = sources_skipped

        direction_sign = 1 if signal.direction == "BUY" else -1
        aligned_score  = composite * direction_sign

        # ── Kill on strongly opposing composite sentiment ──────────────────
        if available_scores and aligned_score <= _KILL_THRESHOLD:
            reason = f"sentiment strongly against {signal.direction} (composite={composite:.3f})"
            signal.reduce(0.12)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={
                    "sentiment_composite": round(composite, 3),
                    "sources_applied": sources_applied,
                    **narrative_data,
                },
            )
            logger.log_pipeline(signal.asset, LAYER, "STRONG_OPPOSE", reason)

        # ── Confidence adjustments ─────────────────────────────────────────
        if aligned_score >= _STRONG_THRESHOLD:
            signal.boost(0.04)
        elif aligned_score <= _WEAK_THRESHOLD:
            signal.reduce(0.03)

        # ── Narrative momentum boost ───────────────────────────────────────
        if narrative_data.get("phase4") == "narrative_ai" and nar_strength > 0.10 and dominant:
            bullish_narratives = {"ETF_NEWS", "HALVING_BUZZ", "AI_TOKENS", "LAYER2_TREND"}
            bearish_narratives = {"REGULATION", "MACRO_SHOCK", "EXCHANGE_NEWS", "STABLECOIN_NEWS"}
            if (signal.direction == "BUY"  and dominant in bullish_narratives) or \
               (signal.direction == "SELL" and dominant in bearish_narratives):
                signal.boost(0.02)
                narrative_data["narrative_boost"] = "+0.02"

        reason = (
            f"composite={composite:+.3f}  "
            f"sources={','.join(sources_applied) or 'none'}  "
            f"narrative={dominant or 'none'}({nar_strength:.2f})"
        )
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={
                "sentiment_composite": round(composite, 3),
                "sources_applied":     sources_applied,
                "sources_skipped":     sources_skipped,
                **narrative_data,
            },
        )
        logger.log_pipeline(
            signal.asset, LAYER, "PASS",
            f"composite={composite:.3f} sources={len(sources_applied)}"
        )
        return signal
