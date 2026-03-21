from __future__ import annotations
import threading
from typing import Any, Dict, Optional
from core.signal import Signal
from core.signal_journal import PASS
from core.asset_profiles import get_profile
from utils.logger import get_logger

logger = get_logger()
LAYER = 5

_STRONG_THRESHOLD = 0.4
_WEAK_THRESHOLD   = -0.3
_KILL_THRESHOLD   = -0.6

_sa_instance = None
_sa_lock     = threading.Lock()

# Narrative themes that are crypto-specific
_CRYPTO_BULLISH_NARRATIVES = {"ETF_NEWS", "HALVING_BUZZ", "AI_TOKENS", "LAYER2_TREND"}
_CRYPTO_BEARISH_NARRATIVES = {"REGULATION", "EXCHANGE_NEWS", "STABLECOIN_NEWS"}

# Narrative themes that apply to all assets
_GENERAL_BULLISH_NARRATIVES = {"MACRO_BOOM", "RISK_ON"}
_GENERAL_BEARISH_NARRATIVES = {"MACRO_SHOCK", "RISK_OFF", "RECESSION"}


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
    """Fetch comprehensive sentiment for asset. Returns 0.0 on failure."""
    try:
        sa = _get_analyzer()
        if sa is None:
            return 0.0
        # get_comprehensive_sentiment takes one optional arg — asset only
        result = sa.get_comprehensive_sentiment(asset)
        if isinstance(result, dict):
            return float(result.get("composite_score", result.get("score", 0.0)))
        return float(result)
    except Exception as e:
        logger.warning(f"[SentimentLayer] Fetch failed for {asset}: {e}")
        return 0.0


def _fetch_put_call(asset: str) -> Optional[float]:
    """Fetch put/call ratio. Only valid for US indices."""
    try:
        sa = _get_analyzer()
        if sa is None:
            return None
        result = sa.fetch_put_call_ratio()
        if result:
            return float(result.get("score", 0.0))
    except Exception:
        pass
    return None


def _fetch_reddit_sentiment(asset: str) -> Optional[float]:
    """
    Fetch Reddit sentiment for any asset.
    First tries the new RedditWatcher.get_asset_sentiment() which supports
    all 18 assets with proper subreddit mappings.
    Falls back to _CryptoSignals.reddit() for crypto if RedditWatcher fails.
    """
    # Primary — new RedditWatcher with per-asset subreddit mappings
    try:
        from reddit_watcher import RedditWatcher
        rw = RedditWatcher()
        result = rw.get_asset_sentiment(asset)
        if result and result.get("total_mentions", 0) > 0:
            score = result.get("score")
            if score is not None:
                return float(score)
    except Exception:
        pass

    # Fallback — _CryptoSignals.reddit() via SentimentAnalyzer (crypto only)
    try:
        sa = _get_analyzer()
        if sa is None or not sa.reddit or not sa.reddit.enabled:
            return None
        result = sa.get_reddit_sentiment_for_asset(asset)
        if result and isinstance(result, dict):
            score = result.get("score")
            if score is None:
                return None
            return float(score)
    except Exception:
        pass
    return None


def _get_narrative_data(asset: str) -> Dict[str, Any]:
    """Pull narrative scores from Phase 4. Returns empty dict on failure."""
    try:
        from narrative_ai import get_narrative_scores, get_dominant_narrative
        scores   = get_narrative_scores()
        dominant = get_dominant_narrative()
        top_score = max(scores.values()) if scores else 0.0
        if not scores:
            logger.debug(f"[SentimentLayer] Narrative AI returned empty scores for {asset}")
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
        profile     = get_profile(signal.asset)

        # ── Sentiment score ───────────────────────────────────────────────
        score = context.get("sentiment_score")
        if score is None:
            score = _fetch_sentiment(signal.asset, signal.category)
        signal.metadata["sentiment_score"] = round(score, 3)

        # ── Phase 4: Narrative data ───────────────────────────────────────
        narrative_data = _get_narrative_data(signal.asset)
        dominant       = narrative_data.get("dominant_narrative", "")
        nar_strength   = narrative_data.get("narrative_strength", 0.0)

        direction_sign = 1 if signal.direction == "BUY" else -1
        aligned_score  = score * direction_sign

        # ── Asset-specific enrichment ─────────────────────────────────────

        # Put/call — US indices only
        if profile.use_put_call:
            pc_score = _fetch_put_call(signal.asset)
            if pc_score is not None:
                signal.metadata["put_call_score"] = round(pc_score, 3)
                pc_aligned = pc_score * direction_sign
                if pc_aligned > 0.3:
                    signal.boost(0.02)
                elif pc_aligned < -0.3:
                    signal.reduce(0.02)

        # Reddit — all assets that have subreddit mappings
        if profile.use_reddit:
            reddit_score = _fetch_reddit_sentiment(signal.asset)
            if reddit_score is not None:
                signal.metadata["reddit_score"] = round(reddit_score, 3)
                reddit_aligned = reddit_score * direction_sign
                if reddit_aligned > 0.3:
                    signal.boost(0.02)
                elif reddit_aligned < -0.3:
                    signal.reduce(0.02)

        # ── Kill on strongly opposing sentiment ───────────────────────────
        if aligned_score <= _KILL_THRESHOLD:
            reason = f"sentiment strongly against {signal.direction} (score={score:.3f})"
            signal.reduce(0.12)
            signal.journal.record(
                layer=LAYER, name=self.name, decision=PASS,
                reason=reason,
                conf_before=conf_before, conf_after=signal.confidence,
                data={"sentiment_score": round(score, 3), **narrative_data},
            )
            logger.log_pipeline(signal.asset, LAYER, "STRONG_OPPOSE", reason)

        # ── General sentiment adjustments ─────────────────────────────────
        if aligned_score >= _STRONG_THRESHOLD:
            signal.boost(0.04)
        elif aligned_score <= _WEAK_THRESHOLD:
            signal.reduce(0.03)

        # ── Narrative momentum boost ──────────────────────────────────────
        if nar_strength > 0.10 and dominant:
            # Crypto-specific narratives — only apply to crypto assets
            if profile.use_whale_data:  # use_whale_data is True only for crypto
                if (signal.direction == "BUY"  and dominant in _CRYPTO_BULLISH_NARRATIVES) or \
                   (signal.direction == "SELL" and dominant in _CRYPTO_BEARISH_NARRATIVES):
                    signal.boost(0.02)
                    narrative_data["narrative_boost"] = "+0.02 (crypto narrative)"

            # General narratives — apply to all asset types
            if (signal.direction == "BUY"  and dominant in _GENERAL_BULLISH_NARRATIVES) or \
               (signal.direction == "SELL" and dominant in _GENERAL_BEARISH_NARRATIVES):
                signal.boost(0.02)
                narrative_data["narrative_boost"] = "+0.02 (macro narrative)"

        reason = (
            f"sentiment={score:+.3f}  "
            f"narrative={dominant or 'none'}({nar_strength:.2f})"
        )
        signal.journal.record(
            layer=LAYER, name=self.name, decision=PASS,
            reason=reason,
            conf_before=conf_before, conf_after=signal.confidence,
            data={"sentiment_score": round(score, 3), **narrative_data},
        )
        logger.log_pipeline(signal.asset, LAYER, "PASS",
                            f"sentiment={score:.3f} narrative={dominant}")
        return signal