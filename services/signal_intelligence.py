from __future__ import annotations

from typing import Any, Dict, List, Optional

from core.asset_profiles import get_profile
from utils.logger import get_logger

logger = get_logger()

_STRONG_THRESHOLD = 0.4
_WEAK_THRESHOLD = -0.3
_KILL_THRESHOLD = -0.6

_CRYPTO_BULLISH_NARRATIVES = {"ETF_NEWS", "HALVING_BUZZ", "AI_TOKENS", "LAYER2_TREND"}
_CRYPTO_BEARISH_NARRATIVES = {"REGULATION", "EXCHANGE_NEWS", "STABLECOIN_NEWS"}
_GENERAL_BULLISH_NARRATIVES = {"MACRO_BOOM", "RISK_ON"}
_GENERAL_BEARISH_NARRATIVES = {"MACRO_SHOCK", "RISK_OFF", "RECESSION"}

def _get_market_intelligence_service():
    try:
        from services.market_intelligence_service import get_service

        return get_service()
    except Exception as exc:
        logger.warning(f"[SignalIntelligence] Market intelligence init failed: {exc}")
        return None


def fetch_sentiment_details(asset: str, category: str) -> Dict[str, Any]:
    try:
        service = _get_market_intelligence_service()
        if service is None:
            return {
                "score": 0.0,
                "composite_score": 0.0,
                "components": {},
                "weights": {},
            }
        return service.get_sentiment_details(asset, category)
    except Exception as exc:
        logger.warning(f"[SignalIntelligence] Sentiment fetch failed for {asset}: {exc}")
        return {
            "score": 0.0,
            "composite_score": 0.0,
            "components": {},
            "weights": {},
        }


def _fetch_put_call(asset: str) -> Optional[float]:
    try:
        service = _get_market_intelligence_service()
        if service is None:
            return None
        return service.get_put_call_score(asset)
    except Exception:
        pass
    return None


def _fetch_reddit_sentiment(asset: str) -> Optional[float]:
    try:
        service = _get_market_intelligence_service()
        if service is None:
            return None
        return service.get_reddit_sentiment_score(asset)
    except Exception:
        pass
    return None


def _get_narrative_data(asset: str) -> Dict[str, Any]:
    service = _get_market_intelligence_service()
    if service is None:
        return {}
    return service.get_narrative_snapshot(asset)


def apply_sentiment_review(signal, context: Dict[str, Any]) -> Dict[str, Any]:
    profile = get_profile(signal.asset)
    intelligence = context.get("market_intelligence")
    sentiment_details = context.get("sentiment_details")
    if not isinstance(sentiment_details, dict) and isinstance(intelligence, dict):
        sentiment_details = intelligence.get("sentiment_details")
    if not isinstance(sentiment_details, dict):
        sentiment_details = fetch_sentiment_details(signal.asset, signal.category)

    score = sentiment_details.get("composite_score", sentiment_details.get("score", 0.0))
    try:
        score = float(score or 0.0)
    except Exception:
        score = 0.0

    components = sentiment_details.get("components", {})
    weights = sentiment_details.get("weights", {})
    market_intelligence_sources: List[str] = []
    market_intelligence_score = None
    market_intelligence_details: Dict[str, Any] = {}
    if isinstance(intelligence, dict):
        market_intelligence_sources = list(intelligence.get("market_intelligence_sources") or [])
        market_intelligence_score = intelligence.get("market_intelligence_score")
        market_intelligence_details = dict(intelligence.get("market_intelligence_details") or {})
    if not isinstance(components, dict):
        components = {}
    if not isinstance(weights, dict):
        weights = {}

    signal.metadata["sentiment_score"] = round(score, 3)
    signal.metadata["sentiment_components"] = {
        str(k): round(float(v), 3) for k, v in components.items()
    }
    signal.metadata["sentiment_weights"] = {
        str(k): round(float(v), 3) for k, v in weights.items()
    }
    if market_intelligence_score is not None:
        try:
            signal.metadata["market_intelligence_score"] = round(float(market_intelligence_score), 3)
        except Exception:
            pass
    if market_intelligence_sources:
        signal.metadata["market_intelligence_sources"] = list(market_intelligence_sources)
    if market_intelligence_details:
        signal.metadata["market_intelligence_details"] = dict(market_intelligence_details)
    if "macro_event" in signal.metadata["sentiment_components"]:
        signal.metadata["macro_sentiment_score"] = signal.metadata["sentiment_components"]["macro_event"]

    sources_used: List[str] = []
    adjustments: List[str] = []
    if abs(score) > 0.01:
        sources_used.append("comprehensive_sentiment")
    if "macro_event" in signal.metadata["sentiment_components"]:
        sources_used.append("macro_event")
    for src in market_intelligence_sources:
        if src not in sources_used:
            sources_used.append(src)

    if isinstance(intelligence, dict):
        narrative_data = {
            "dominant_narrative": intelligence.get("dominant_narrative", ""),
            "narrative_strength": intelligence.get("narrative_strength", 0.0),
        }
    else:
        narrative_data = _get_narrative_data(signal.asset)
    dominant = narrative_data.get("dominant_narrative", "")
    nar_strength = float(narrative_data.get("narrative_strength", 0.0) or 0.0)

    direction_sign = 1 if signal.direction == "BUY" else -1
    aligned_score = score * direction_sign

    if profile.use_put_call:
        pc_score = components.get("put_call")
        if pc_score is None:
            pc_score = _fetch_put_call(signal.asset)
        if pc_score is not None:
            signal.metadata["put_call_score"] = round(pc_score, 3)
            sources_used.append("put_call")
            pc_aligned = pc_score * direction_sign
            if pc_aligned > 0.3:
                adjustments.append("put_call_support")
            elif pc_aligned < -0.3:
                adjustments.append("put_call_conflict")

    if profile.use_reddit:
        reddit_score = components.get("reddit")
        if reddit_score is None:
            reddit_score = _fetch_reddit_sentiment(signal.asset)
        if reddit_score is not None:
            signal.metadata["reddit_score"] = round(reddit_score, 3)
            sources_used.append("reddit")
            reddit_aligned = reddit_score * direction_sign
            if reddit_aligned > 0.3:
                adjustments.append("reddit_support")
            elif reddit_aligned < -0.3:
                adjustments.append("reddit_conflict")

    if aligned_score <= _KILL_THRESHOLD:
        adjustments.append("strong_opposition")

    if aligned_score >= _STRONG_THRESHOLD:
        adjustments.append("sentiment_support")
    elif aligned_score <= _WEAK_THRESHOLD:
        adjustments.append("sentiment_conflict")

    if nar_strength > 0.10 and dominant:
        sources_used.append("narrative_ai")
        if profile.use_whale_data:
            if (signal.direction == "BUY" and dominant in _CRYPTO_BULLISH_NARRATIVES) or (
                signal.direction == "SELL" and dominant in _CRYPTO_BEARISH_NARRATIVES
            ):
                adjustments.append("crypto_narrative_support")

        if (signal.direction == "BUY" and dominant in _GENERAL_BULLISH_NARRATIVES) or (
            signal.direction == "SELL" and dominant in _GENERAL_BEARISH_NARRATIVES
        ):
            adjustments.append("macro_narrative_support")

    signal.metadata["sentiment_sources"] = sources_used

    return {
        "score": round(score, 3),
        "sources": sources_used,
        "components": signal.metadata["sentiment_components"],
        "weights": signal.metadata["sentiment_weights"],
        "dominant_narrative": dominant,
        "narrative_strength": round(nar_strength, 3),
        "adjustments": adjustments,
        "market_intelligence_sources": list(market_intelligence_sources),
    }


def apply_whale_review(signal, context: Dict[str, Any]) -> Dict[str, Any]:
    profile = get_profile(signal.asset)
    if not profile.use_whale_data:
        signal.metadata["whale_skipped"] = True
        return {"applicable": False, "reason": "non_crypto"}

    intelligence = context.get("market_intelligence")
    snapshot = intelligence.get("whale_snapshot") if isinstance(intelligence, dict) else None
    if not isinstance(snapshot, dict):
        service = _get_market_intelligence_service()
        snapshot = service.get_whale_snapshot(signal.asset) if service is not None else {}

    buy_vol = float(snapshot.get("buy_vol_m", 0.0) or 0.0) * 1_000_000
    sell_vol = float(snapshot.get("sell_vol_m", 0.0) or 0.0) * 1_000_000
    total = buy_vol + sell_vol

    signal.metadata["whale_buy_vol"] = buy_vol
    signal.metadata["whale_sell_vol"] = sell_vol

    if not snapshot.get("has_data", False):
        signal.metadata["whale_skipped"] = True
        return {
            "applicable": True,
            "reason": snapshot.get("reason", "no_data"),
            "buy_vol_m": round(buy_vol / 1e6, 2),
            "sell_vol_m": round(sell_vol / 1e6, 2),
        }

    signal.metadata["whale_data"] = "real"
    dominant = snapshot.get("dominant")
    ratio = float(snapshot.get("ratio", 0.5) or 0.5)
    weighted_bull = float(snapshot.get("weighted_bull", 0.0) or 0.0)
    weighted_bear = float(snapshot.get("weighted_bear", 0.0) or 0.0)
    clusters = int(snapshot.get("clusters", 0) or 0)

    signal.metadata["whale_dominant"] = dominant
    signal.metadata["whale_ratio"] = round(ratio, 3)
    signal.metadata["whale_bull_weight"] = round(weighted_bull, 3)
    signal.metadata["whale_bear_weight"] = round(weighted_bear, 3)
    if snapshot.get("source_breakdown"):
        signal.metadata["whale_sources"] = dict(snapshot.get("source_breakdown", {}))

    adjustments: List[str] = []
    if dominant != signal.direction and ratio > 0.65:
        penalty = 0.10 + min(0.10, ratio * 0.15)
        adjustments.append(f"whale_conflict={penalty:.3f}")

    boost = 0.0
    if dominant == signal.direction:
        vol_m = total / 1_000_000
        vol_factor = min(1.0, vol_m / 50)
        boost = min(0.12, ratio * 0.1 + vol_factor * 0.04)
        if clusters > 0:
            boost = min(0.15, boost + 0.03)
        adjustments.append(f"whale_support={boost:.3f}")

    return {
        "applicable": True,
        "dominant": dominant,
        "ratio": round(ratio, 3),
        "buy_vol_m": round(buy_vol / 1e6, 2),
        "sell_vol_m": round(sell_vol / 1e6, 2),
        "clusters": clusters,
        "weighted_bull": round(weighted_bull, 3),
        "weighted_bear": round(weighted_bear, 3),
        "adjustments": adjustments,
        "phase2": snapshot.get("phase2", "unavailable"),
    }
