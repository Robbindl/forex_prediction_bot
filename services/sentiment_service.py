from __future__ import annotations

"""Public sentiment orchestration service for the bot."""

import threading
from datetime import datetime
from typing import Any, Dict, Optional

from services.sentiment_sources import (
    _CryptoSignals,
    _MarketInstruments,
    _NewsSentiment,
    _PriceMomentum,
    _cat,
    _clamp,
    _reddit_score,
)
from utils.logger import get_logger

logger = get_logger()

# ══════════════════════════════════════════════════════════════════════════════
# Main sentiment service
# ══════════════════════════════════════════════════════════════════════════════

class SentimentService:
    """
    Runtime sentiment engine for signal generation.
    Source collection lives in ``services.sentiment_sources``.
    """

    def __init__(self):
        logger.info("[SentimentService] initialised")

    # ── Core method — called by the decision engine and dashboard ────────────

    def get_comprehensive_sentiment(self, asset: str = None) -> Dict:
        """
        Return sentiment for a specific asset, or global market sentiment.
        Score: -1.0 (max bearish) … +1.0 (max bullish).
        """
        if asset is None:
            return self._global_sentiment()

        cat = _cat(asset)
        if cat == "crypto":
            return self._crypto_sentiment(asset)
        elif cat == "commodities":
            return self._commodity_sentiment(asset)
        elif cat == "forex":
            return self._forex_sentiment(asset)
        else:
            return self._index_sentiment(asset)

    def _global_sentiment(self) -> Dict:
        """Global market composite — used by command center."""
        components: Dict[str, float] = {}

        fg = _MarketInstruments.fear_greed()
        if fg:
            components["fear_greed"] = fg["score"]

        vix = _MarketInstruments.vix()
        if vix:
            components["vix"] = vix["score"]

        score = (sum(components.values()) / len(components)) if components else 0.0
        return {
            "score":           round(_clamp(score), 3),
            "composite_score": round(_clamp(score), 3),
            "interpretation":  self._interpret(score),
            "components":      components,
            "timestamp":       datetime.now().isoformat(),
        }

    def _crypto_sentiment(self, asset: str) -> Dict:
        components: Dict[str, float] = {}
        weights   : Dict[str, float] = {}

        # 1. Crypto Fear & Greed (most reliable for crypto)
        fg = _MarketInstruments.fear_greed()
        if fg:
            components["fear_greed"] = fg["score"]
            weights["fear_greed"]    = 0.30

        # 2. Price momentum
        pm = _PriceMomentum.get(asset)
        if pm is not None:
            components["price_momentum"] = pm
            weights["price_momentum"]    = 0.30

        # 3. News sentiment (asset-filtered)
        ns = _NewsSentiment.get(asset)
        if ns is not None:
            components["news"] = ns
            weights["news"]    = 0.20

        # 4. Reddit — uses new public-JSON watcher (all subreddits per asset)
        rd = _reddit_score(asset)
        if rd is not None:
            components["reddit"] = rd
            weights["reddit"]    = 0.20

        macro = _NewsSentiment.macro_impact(asset)
        if macro is not None:
            components["macro_event"] = macro
            weights["macro_event"]    = 0.10

        return self._build_result(components, weights)

    def _commodity_sentiment(self, asset: str) -> Dict:
        components: Dict[str, float] = {}
        weights   : Dict[str, float] = {}
        ig_client_sentiment: Optional[Dict[str, Any]] = None

        # 1. Price momentum — most reliable for commodities
        pm = _PriceMomentum.get(asset)
        if pm is not None:
            components["price_momentum"] = pm
            weights["price_momentum"]    = 0.35

        # 2. News (asset-specific — precious-metals coverage)
        ns = _NewsSentiment.get(asset)
        if ns is not None:
            components["news"] = ns
            weights["news"]    = 0.30

        # 3. Reddit — metals-focused subreddits via public JSON
        rd = _reddit_score(asset)
        if rd is not None:
            components["reddit"] = rd
            weights["reddit"]    = 0.15

        macro = _NewsSentiment.macro_impact(asset)
        if macro is not None:
            components["macro_event"] = macro
            weights["macro_event"]    = 0.10

        # 4. VIX (risk-off tends to support safe-haven metals)
        vix = _MarketInstruments.vix()
        if vix:
            # High VIX can support gold/silver as defensive assets.
            v_score = vix["score"]
            if asset in {"XAU/USD", "GC=F"}:
                v_score = -v_score  # invert — gold benefits from fear
            elif asset in {"XAG/USD", "SI=F"}:
                v_score = -v_score * 0.7
            components["vix"] = v_score
            weights["vix"]    = 0.15

        ig_client_sentiment = self._ig_client_sentiment(asset)
        if ig_client_sentiment:
            components["ig_client_sentiment"] = float(ig_client_sentiment.get("score", 0.0) or 0.0)
            weights["ig_client_sentiment"] = 0.10

        result = self._build_result(components, weights)
        if ig_client_sentiment:
            result["ig_client_sentiment"] = dict(ig_client_sentiment)
        return result

    def _forex_sentiment(self, asset: str) -> Dict:
        components: Dict[str, float] = {}
        weights   : Dict[str, float] = {}

        # 1. Price momentum
        pm = _PriceMomentum.get(asset)
        if pm is not None:
            components["price_momentum"] = pm
            weights["price_momentum"]    = 0.40

        # 2. News (asset-filtered)
        ns = _NewsSentiment.get(asset)
        if ns is not None:
            components["news"] = ns
            weights["news"]    = 0.30

        # 3. Reddit — r/Forex, r/Forexstrategy, r/trading via public JSON
        rd = _reddit_score(asset)
        if rd is not None:
            components["reddit"] = rd
            weights["reddit"]    = 0.15

        macro = _NewsSentiment.macro_impact(asset)
        if macro is not None:
            components["macro_event"] = macro
            weights["macro_event"]    = 0.10

        # 4. VIX — high VIX = risk-off = USD strength = bearish non-USD pairs
        vix = _MarketInstruments.vix()
        if vix:
            components["vix"] = vix["score"]
            weights["vix"]    = 0.20
        return self._build_result(components, weights)

    def _index_sentiment(self, asset: str) -> Dict:
        """US/UK equity indices — VIX and Fear & Greed are most reliable."""
        components: Dict[str, float] = {}
        weights   : Dict[str, float] = {}

        # 1. Price momentum
        pm = _PriceMomentum.get(asset)
        if pm is not None:
            components["price_momentum"] = pm
            weights["price_momentum"]    = 0.20

        # 2. Reddit — equity-index subreddits via public JSON
        rd = _reddit_score(asset)
        if rd is not None:
            components["reddit"] = rd
            weights["reddit"]    = 0.10

        # 3. VIX (primary fear gauge for equities)
        vix = _MarketInstruments.vix()
        if vix:
            components["vix"] = vix["score"]
            weights["vix"]    = 0.30

        # 3. Fear & Greed
        fg = _MarketInstruments.fear_greed()
        if fg:
            components["fear_greed"] = fg["score"]
            weights["fear_greed"]    = 0.20

        macro = _NewsSentiment.macro_impact(asset)
        if macro is not None:
            components["macro_event"] = macro
            weights["macro_event"]    = 0.10

        # 4. AAII (weekly survey)
        aaii = _MarketInstruments.aaii()
        if aaii:
            components["aaii"] = aaii["score"]
            weights["aaii"]    = 0.10

        # 5. Put/Call ratio
        pc = _MarketInstruments.put_call()
        if pc:
            components["put_call"] = pc["score"]
            weights["put_call"]    = 0.10
        return self._build_result(components, weights)

    @staticmethod
    def _build_result(components: Dict[str, float], weights: Dict[str, float]) -> Dict:
        if not components:
            score = 0.0
        else:
            total_w = sum(weights.get(k, 0.2) for k in components)
            score   = sum(v * weights.get(k, 0.2) for k, v in components.items()) / max(total_w, 0.01)
            score   = _clamp(score)
        return {
            "score":           round(score, 3),
            "composite_score": round(score, 3),
            "interpretation":  SentimentService._interpret(score),
            "components":      {k: round(v, 3) for k, v in components.items()},
            "weights":         {k: round(weights.get(k, 0.0), 3) for k in components},
            "timestamp":       datetime.now().isoformat(),
        }

    @staticmethod
    def _interpret(score: float) -> str:
        if score >  0.4: return "Strongly Bullish"
        if score >  0.1: return "Bullish"
        if score > -0.1: return "Neutral"
        if score > -0.4: return "Bearish"
        return "Strongly Bearish"

    def fetch_put_call_ratio(self) -> Optional[Dict]:
        return _MarketInstruments.put_call()

    def get_reddit_sentiment_for_asset(self, asset: str) -> Optional[Dict]:
        score = _CryptoSignals.reddit(asset)
        if score is None:
            return None
        return {"score": score, "total_posts": 0, "asset": asset}

    @staticmethod
    def _ig_client_sentiment(asset: str) -> Optional[Dict[str, Any]]:
        try:
            from services.market_data_router import get_client_sentiment

            data = get_client_sentiment(asset, category="commodities")
            return data if isinstance(data, dict) and data else None
        except Exception:
            return None


# ── Module-level singleton ────────────────────────────────────────────────────

_instance:   Optional[SentimentService] = None
_inst_lock   = threading.Lock()


def get_service() -> SentimentService:
    global _instance
    if _instance is not None:
        return _instance
    with _inst_lock:
        if _instance is None:
            _instance = SentimentService()
    return _instance
