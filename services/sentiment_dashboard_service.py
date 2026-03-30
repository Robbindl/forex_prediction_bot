from __future__ import annotations

"""Dashboard adapter for sentiment and whale display helpers."""

import threading
from typing import Dict, List, Optional

from services.sentiment_service import SentimentService, get_service as get_sentiment_service
from services.sentiment_sources import _CryptoSignals, _MarketInstruments, _NewsSentiment
from utils.logger import get_logger

logger = get_logger()


class _NewsIntegratorShim:
    """Provides dashboard news access without bloating the runtime service."""

    def fetch_all_sources(self) -> List[Dict]:
        return _NewsSentiment.get_articles_for_dashboard(limit=20)

    def get_sentiment_summary(self, asset: str = None) -> Dict:
        service = get_dashboard_service()
        if asset:
            return service.get_comprehensive_sentiment(asset)
        return service.get_comprehensive_sentiment()


class SentimentDashboardService:
    """Dashboard-facing sentiment adapter with lazy external dependencies."""

    def __init__(self, service: Optional[SentimentService] = None):
        self._service = service or get_sentiment_service()
        self.news_integrator = _NewsIntegratorShim()
        self._market_intelligence = None
        self._market_intelligence_attempted = False
        self._lock = threading.Lock()

    def get_comprehensive_sentiment(self, asset: str = None) -> Dict:
        return self._service.get_comprehensive_sentiment(asset)

    def fetch_fear_greed_index(self) -> Dict:
        fg = _MarketInstruments.fear_greed()
        if fg:
            return fg
        return {"value": 50, "classification": "Neutral", "score": 0.0}

    def fetch_vix(self) -> Dict:
        vix = _MarketInstruments.vix()
        if vix:
            return vix
        return {"value": 20.0, "classification": "Normal", "score": 0.0}

    def fetch_aaii_sentiment(self) -> Dict:
        aaii = _MarketInstruments.aaii()
        if aaii:
            return aaii
        return {"bullish": 38.0, "bearish": 30.0, "spread": 8.0, "score": 0.1}

    def fetch_put_call_ratio(self) -> Optional[Dict]:
        return self._service.fetch_put_call_ratio()

    def fetch_cnn_fear_greed(self) -> Dict:
        return self.fetch_fear_greed_index()

    def fetch_whale_alerts(self, min_value_usd: float = 1_000_000) -> List[Dict]:
        intelligence = self._get_market_intelligence()
        if intelligence is None:
            return []
        try:
            return intelligence.get_whale_events(
                min_value_usd=min_value_usd,
                hours=24,
                limit=20,
            ) or []
        except Exception:
            return []

    def get_reddit_sentiment_for_asset(self, asset: str) -> Optional[Dict]:
        return self._service.get_reddit_sentiment_for_asset(asset)

    def get_reddit_sentiment(self) -> Dict:
        return {"score": 0.0, "total_posts": 0}

    def get_best_sentiment(self, asset: str, days: int = 1) -> Optional[Dict]:
        return self.get_comprehensive_sentiment(asset)

    def get_market_events(self) -> Dict:
        intelligence = self._get_market_intelligence()
        if intelligence is None:
            return {"events": [], "earnings": [], "halving": {}, "risk_outlook": {}}
        try:
            return intelligence.get_market_events(days=7, limit=8)
        except Exception as exc:
            logger.debug(f"[SentimentDashboard] Market events: {exc}")
            return {"events": [], "earnings": [], "halving": {}, "risk_outlook": {}}

    def fetch_general_news_sentiment(self) -> Dict:
        articles = _NewsSentiment.get_articles_for_dashboard(limit=20)
        if not articles:
            return {"score": 0.0, "interpretation": "Neutral", "article_count": 0}
        scores = [a["sentiment"] for a in articles if a.get("sentiment") is not None]
        avg = sum(scores) / len(scores) if scores else 0.0
        return {
            "score": round(avg, 3),
            "interpretation": self._service._interpret(avg),
            "article_count": len(articles),
        }

    def fetch_onchain_metrics(self) -> Dict:
        score = _CryptoSignals.onchain() or 0.0
        return {
            "combined_score": score,
            "interpretation": self._service._interpret(score),
        }

    def fetch_crypto_news_sentiment(self, asset: str = "general") -> Dict:
        score = _NewsSentiment.get("BTC-USD") or 0.0
        return {"score": score, "interpretation": self._service._interpret(score)}

    def _get_market_intelligence(self):
        if self._market_intelligence_attempted:
            return self._market_intelligence
        with self._lock:
            if self._market_intelligence_attempted:
                return self._market_intelligence
            self._market_intelligence_attempted = True
            try:
                from services.market_intelligence_service import get_service as get_market_intelligence_service

                self._market_intelligence = get_market_intelligence_service()
            except Exception as exc:
                logger.debug(f"[SentimentDashboard] MarketIntelligence unavailable: {exc}")
                self._market_intelligence = None
        return self._market_intelligence


_dashboard_instance: Optional[SentimentDashboardService] = None
_dashboard_lock = threading.Lock()


def get_dashboard_service() -> SentimentDashboardService:
    global _dashboard_instance
    if _dashboard_instance is not None:
        return _dashboard_instance
    with _dashboard_lock:
        if _dashboard_instance is None:
            _dashboard_instance = SentimentDashboardService()
    return _dashboard_instance


__all__ = ["SentimentDashboardService", "get_dashboard_service"]
