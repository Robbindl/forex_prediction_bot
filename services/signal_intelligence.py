from __future__ import annotations

from typing import Any, Dict, List, Optional

from config.config import NEWS_REDDIT_ENABLED, NEWS_SENTIMENT_EXECUTION_ENABLED
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

_EXECUTION_SENTIMENT_COMPONENTS = {
    "price_momentum",
    "fear_greed",
    "vix",
    "ig_client_sentiment",
    "put_call",
    "aaii",
}

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


def _resolve_sentiment_review_context(
    signal,
    context: Dict[str, Any],
) -> tuple[Dict[str, Any], List[str], Any, Dict[str, Any], Dict[str, Any], Dict[str, Any], str, str]:
    intelligence = context.get("market_intelligence")
    sentiment_details = context.get("sentiment_details")
    if not isinstance(sentiment_details, dict) and isinstance(intelligence, dict):
        sentiment_details = intelligence.get("sentiment_details")
    if not isinstance(sentiment_details, dict):
        sentiment_details = fetch_sentiment_details(signal.asset, signal.category)

    if isinstance(intelligence, dict):
        free_market_intelligence = intelligence.get("free_market_intelligence")
        if not isinstance(free_market_intelligence, dict):
            free_market_intelligence = {}
        market_intelligence_sources = list(intelligence.get("market_intelligence_sources") or [])
        market_intelligence_score = intelligence.get("market_intelligence_score")
        market_intelligence_details = dict(intelligence.get("market_intelligence_details") or {})
        market_intelligence_components = dict(
            free_market_intelligence.get("components")
            or intelligence.get("market_intelligence_components")
            or {}
        )
        market_intelligence_timestamp = str(
            free_market_intelligence.get("timestamp")
            or intelligence.get("market_intelligence_timestamp")
            or intelligence.get("intelligence_timestamp")
            or ""
        )
        intelligence_timestamp = str(intelligence.get("intelligence_timestamp") or market_intelligence_timestamp or "")
        narrative_data = {
            "dominant_narrative": intelligence.get("dominant_narrative", ""),
            "narrative_strength": intelligence.get("narrative_strength", 0.0),
        }
    else:
        market_intelligence_sources = []
        market_intelligence_score = None
        market_intelligence_details = {}
        market_intelligence_components = {}
        market_intelligence_timestamp = ""
        intelligence_timestamp = ""
        narrative_data = _get_narrative_data(signal.asset)

    return (
        sentiment_details,
        market_intelligence_sources,
        market_intelligence_score,
        market_intelligence_details,
        narrative_data,
        market_intelligence_components,
        market_intelligence_timestamp,
        intelligence_timestamp,
    )


def _sentiment_review_sources(
    *,
    score: float,
    ig_client_sentiment: Dict[str, Any] | None,
    components: Dict[str, Any],
    market_intelligence_sources: List[str],
    dominant: str,
    nar_strength: float,
) -> List[str]:
    sources_used: List[str] = []
    if abs(score) > 0.01:
        sources_used.append("comprehensive_sentiment")
    if isinstance(ig_client_sentiment, dict) and ig_client_sentiment:
        sources_used.append("ig_client_sentiment")
    if "macro_event" in components:
        sources_used.append("macro_event")
    for src in market_intelligence_sources:
        if src not in sources_used:
            sources_used.append(src)
    if nar_strength > 0.10 and dominant:
        sources_used.append("narrative_ai")
    return sources_used


def _sentiment_review_attach_metadata(
    signal,
    *,
    raw_score: float,
    execution_score: float,
    execution_mode: str,
    execution_components: Dict[str, Any],
    components: Dict[str, Any],
    weights: Dict[str, Any],
    ig_client_sentiment: Dict[str, Any] | None,
    market_intelligence_score: Any,
    market_intelligence_sources: List[str],
    market_intelligence_details: Dict[str, Any],
    market_intelligence_components: Dict[str, Any],
    sentiment_timestamp: str,
    market_intelligence_timestamp: str,
    intelligence_timestamp: str,
) -> None:
    signal.metadata["sentiment_score"] = round(execution_score, 3)
    signal.metadata["sentiment_raw_score"] = round(raw_score, 3)
    signal.metadata["sentiment_execution_mode"] = execution_mode
    signal.metadata["sentiment_execution_components"] = {
        str(k): round(float(v), 3) for k, v in execution_components.items()
    }
    signal.metadata["sentiment_components"] = {str(k): round(float(v), 3) for k, v in components.items()}
    signal.metadata["sentiment_weights"] = {str(k): round(float(v), 3) for k, v in weights.items()}

    if isinstance(ig_client_sentiment, dict) and ig_client_sentiment:
        signal.metadata["ig_client_sentiment"] = dict(ig_client_sentiment)
    if market_intelligence_score is not None:
        try:
            signal.metadata["market_intelligence_score"] = round(float(market_intelligence_score), 3)
        except Exception:
            pass
    if market_intelligence_sources:
        signal.metadata["market_intelligence_sources"] = list(market_intelligence_sources)
    if market_intelligence_details:
        signal.metadata["market_intelligence_details"] = dict(market_intelligence_details)
    if market_intelligence_components:
        signal.metadata["market_intelligence_components"] = {
            str(k): round(float(v), 3) for k, v in market_intelligence_components.items()
        }
    if sentiment_timestamp:
        signal.metadata["sentiment_timestamp"] = str(sentiment_timestamp)
    if market_intelligence_timestamp:
        signal.metadata["market_intelligence_timestamp"] = str(market_intelligence_timestamp)
    if intelligence_timestamp:
        signal.metadata["intelligence_timestamp"] = str(intelligence_timestamp)
    if "macro_event" in signal.metadata["sentiment_components"]:
        signal.metadata["macro_sentiment_score"] = signal.metadata["sentiment_components"]["macro_event"]


def _resolve_execution_sentiment_score(
    raw_score: float,
    components: Dict[str, Any],
    weights: Dict[str, Any],
) -> tuple[float, str, Dict[str, Any]]:
    if NEWS_SENTIMENT_EXECUTION_ENABLED:
        return raw_score, "full_sentiment", dict(components)

    reliable: Dict[str, Any] = {}
    for name, value in components.items():
        if str(name) in _EXECUTION_SENTIMENT_COMPONENTS:
            reliable[str(name)] = value

    if not reliable:
        return 0.0, "news_dashboard_only", {}

    total_weight = 0.0
    weighted_score = 0.0
    for name, value in reliable.items():
        try:
            component_value = float(value or 0.0)
        except Exception:
            continue
        try:
            weight = float(weights.get(name, 0.2) or 0.2)
        except Exception:
            weight = 0.2
        total_weight += max(0.0, weight)
        weighted_score += component_value * max(0.0, weight)

    if total_weight <= 0.0:
        return 0.0, "news_dashboard_only", reliable
    score = max(-1.0, min(1.0, weighted_score / total_weight))
    return score, "reliable_components_only", reliable


def _sentiment_review_signal_adjustments(
    *,
    signal,
    profile,
    score: float,
    components: Dict[str, Any],
) -> List[str]:
    adjustments: List[str] = []
    direction_sign = 1 if signal.direction == "BUY" else -1
    aligned_score = score * direction_sign

    if profile.use_put_call:
        pc_score = components.get("put_call")
        if pc_score is None:
            pc_score = _fetch_put_call(signal.asset)
        if pc_score is not None:
            signal.metadata["put_call_score"] = round(pc_score, 3)
            pc_aligned = pc_score * direction_sign
            if pc_aligned > 0.3:
                adjustments.append("put_call_support")
            elif pc_aligned < -0.3:
                adjustments.append("put_call_conflict")

    if profile.use_reddit and NEWS_REDDIT_ENABLED:
        reddit_score = components.get("reddit")
        if reddit_score is None:
            reddit_score = _fetch_reddit_sentiment(signal.asset)
        if reddit_score is not None:
            signal.metadata["reddit_score"] = round(reddit_score, 3)
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

    return adjustments


def _sentiment_review_narrative_adjustments(
    *,
    signal,
    profile,
    dominant: str,
    nar_strength: float,
) -> List[str]:
    adjustments: List[str] = []
    if nar_strength > 0.10 and dominant:
        if profile.use_whale_data:
            if (signal.direction == "BUY" and dominant in _CRYPTO_BULLISH_NARRATIVES) or (
                signal.direction == "SELL" and dominant in _CRYPTO_BEARISH_NARRATIVES
            ):
                adjustments.append("crypto_narrative_support")

        if (signal.direction == "BUY" and dominant in _GENERAL_BULLISH_NARRATIVES) or (
            signal.direction == "SELL" and dominant in _GENERAL_BEARISH_NARRATIVES
        ):
            adjustments.append("macro_narrative_support")
    return adjustments


def _sentiment_review_adjustments(
    *,
    signal,
    profile,
    score: float,
    components: Dict[str, Any],
    dominant: str,
    nar_strength: float,
) -> List[str]:
    adjustments = _sentiment_review_signal_adjustments(
        signal=signal,
        profile=profile,
        score=score,
        components=components,
    )
    adjustments.extend(
        _sentiment_review_narrative_adjustments(
            signal=signal,
            profile=profile,
            dominant=dominant,
            nar_strength=nar_strength,
        )
    )
    return adjustments


def apply_sentiment_review(signal, context: Dict[str, Any]) -> Dict[str, Any]:
    profile = get_profile(signal.asset)
    (
        sentiment_details,
        market_intelligence_sources,
        market_intelligence_score,
        market_intelligence_details,
        narrative_data,
        market_intelligence_components,
        market_intelligence_timestamp,
        intelligence_timestamp,
    ) = _resolve_sentiment_review_context(signal, context)

    score = sentiment_details.get("composite_score", sentiment_details.get("score", 0.0))
    try:
        score = float(score or 0.0)
    except Exception:
        score = 0.0

    components = sentiment_details.get("components", {})
    weights = sentiment_details.get("weights", {})
    if not isinstance(components, dict):
        components = {}
    if not isinstance(weights, dict):
        weights = {}

    ig_client_sentiment = sentiment_details.get("ig_client_sentiment")
    execution_score, execution_mode, execution_components = _resolve_execution_sentiment_score(score, components, weights)

    _sentiment_review_attach_metadata(
        signal,
        raw_score=score,
        execution_score=execution_score,
        execution_mode=execution_mode,
        execution_components=execution_components,
        components=components,
        weights=weights,
        ig_client_sentiment=ig_client_sentiment,
        market_intelligence_score=market_intelligence_score,
        market_intelligence_sources=market_intelligence_sources,
        market_intelligence_details=market_intelligence_details,
        market_intelligence_components=market_intelligence_components,
        sentiment_timestamp=str(sentiment_details.get("timestamp", "") or ""),
        market_intelligence_timestamp=market_intelligence_timestamp,
        intelligence_timestamp=intelligence_timestamp,
    )
    dominant = narrative_data.get("dominant_narrative", "")
    nar_strength = float(narrative_data.get("narrative_strength", 0.0) or 0.0)

    sources_used = _sentiment_review_sources(
        score=score,
        ig_client_sentiment=ig_client_sentiment,
        components=components,
        market_intelligence_sources=market_intelligence_sources,
        dominant=dominant,
        nar_strength=nar_strength,
    )
    adjustments = _sentiment_review_adjustments(
        signal=signal,
        profile=profile,
        score=execution_score,
        components=components,
        dominant=dominant,
        nar_strength=nar_strength,
    )

    signal.metadata["sentiment_sources"] = sources_used

    return {
        "score": round(execution_score, 3),
        "raw_score": round(score, 3),
        "execution_mode": execution_mode,
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

    if isinstance(intelligence, dict):
        intelligence_timestamp = str(intelligence.get("intelligence_timestamp") or "")
        if intelligence_timestamp:
            signal.metadata["intelligence_timestamp"] = intelligence_timestamp
            signal.metadata["whale_timestamp"] = intelligence_timestamp

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


def apply_cross_asset_review(signal, context: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = context.get("cross_asset_context")
    if not isinstance(snapshot, dict) or not snapshot:
        try:
            fetcher = context.get("fetcher")
            if fetcher is not None:
                from services.cross_asset_spillover_service import get_service as get_cross_asset_spillover_service

                snapshot = get_cross_asset_spillover_service().build_snapshot(
                    asset=signal.asset,
                    category=signal.category,
                    fetcher=fetcher,
                    timeframe=str(context.get("timeframe") or "15m"),
                )
            else:
                snapshot = {}
        except Exception as exc:
            logger.debug(f"[SignalIntelligence] Cross-asset fetch failed for {signal.asset}: {exc}")
            snapshot = {}

    if not isinstance(snapshot, dict) or not snapshot:
        signal.metadata["cross_asset_score"] = 0.0
        signal.metadata["cross_asset_alignment"] = 0.0
        signal.metadata["cross_asset_confidence"] = 0.0
        return {"applicable": False, "score": 0.0, "alignment": 0.0, "peers": []}

    raw_score = float(snapshot.get("score", 0.0) or 0.0)
    confidence = float(snapshot.get("confidence", 0.0) or 0.0)
    aligned = raw_score if signal.direction == "BUY" else -raw_score

    signal.metadata["cross_asset_context"] = dict(snapshot)
    signal.metadata["cross_asset_score"] = round(raw_score, 4)
    signal.metadata["cross_asset_alignment"] = round(aligned, 4)
    signal.metadata["cross_asset_confidence"] = round(confidence, 4)
    signal.metadata["cross_asset_state"] = str(snapshot.get("state", "") or "")
    signal.metadata["cross_asset_supportive_direction"] = str(snapshot.get("supportive_direction", "") or "")
    signal.metadata["cross_asset_primary_peer"] = str(snapshot.get("dominant_peer", "") or "")
    signal.metadata["cross_asset_primary_relation"] = str(snapshot.get("dominant_relation", "") or "")
    signal.metadata["cross_asset_peer_count"] = int(len(snapshot.get("peers") or []))

    peers: List[Dict[str, Any]] = []
    for peer in list(snapshot.get("peers") or [])[:3]:
        if not isinstance(peer, dict):
            continue
        peers.append(
            {
                "peer_asset": str(peer.get("peer_asset", "") or ""),
                "relation_label": str(peer.get("relation_label", "") or ""),
                "supportive_direction": str(peer.get("supportive_direction", "") or ""),
                "peer_direction": str(peer.get("peer_direction", "") or ""),
                "buy_bias": round(float(peer.get("buy_bias", 0.0) or 0.0), 4),
                "weight": round(float(peer.get("weight", 0.0) or 0.0), 4),
                "state": str(peer.get("state", "") or ""),
            }
        )

    adjustments: List[str] = []
    if aligned >= 0.20:
        adjustments.append("cross_asset_support")
    elif aligned <= -0.20:
        adjustments.append("cross_asset_conflict")
    if confidence >= 0.70:
        adjustments.append("cross_asset_high_confidence")

    return {
        "applicable": True,
        "score": round(raw_score, 4),
        "alignment": round(aligned, 4),
        "confidence": round(confidence, 4),
        "state": str(snapshot.get("state", "") or ""),
        "supportive_direction": str(snapshot.get("supportive_direction", "") or ""),
        "dominant_peer": str(snapshot.get("dominant_peer", "") or ""),
        "dominant_relation": str(snapshot.get("dominant_relation", "") or ""),
        "peers": peers,
        "adjustments": adjustments,
    }
