from __future__ import annotations

"""Consolidated market-intelligence snapshots for signal evaluation."""

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from utils.display_time import display_timezone_label, to_display_datetime
from utils.logger import get_logger

logger = get_logger()

_WHALE_EVENT_TTL = timedelta(hours=1)
_ONCHAIN_EVENT_TTL = timedelta(hours=2)

_SOURCE_CONFIDENCE = {
    "whale-alert.io": 0.95,
    "whale_alert": 0.95,
    "whalealert": 0.95,
    "telegram": 0.85,
    "twitter": 0.70,
    "reddit": 0.55,
    "on-chain": 0.90,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_timestamp(value: Any = None) -> datetime:
    if value is None:
        return _utc_now()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 1_000_000_000_000:
            raw /= 1000.0
        return datetime.fromtimestamp(raw, timezone.utc)
    if isinstance(value, str):
        try:
            normalized = value.strip()
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass
        try:
            return datetime.fromtimestamp(float(value), timezone.utc)
        except Exception:
            pass
    return _utc_now()


def _asset_base(asset: str) -> str:
    return (asset or "").upper().replace("-USD", "").replace("-USDT", "").replace("/", "").replace("_", "")


def _match_asset(asset: str, candidate: str) -> bool:
    base = _asset_base(asset)
    other = _asset_base(candidate)
    if not base or not other:
        return False
    return base in other or other in base


def _normalize_direction(value: Any, fallback_sentiment: float = 0.0) -> str:
    raw = str(value or "").upper().strip()
    if raw in {"BUY", "BULL", "BULLISH", "LONG", "ACCUMULATION", "INFLOW"}:
        return "BUY"
    if raw in {"SELL", "BEAR", "BEARISH", "SHORT", "DISTRIBUTION", "OUTFLOW"}:
        return "SELL"
    return "BUY" if float(fallback_sentiment or 0.0) >= 0.0 else "SELL"


def _ping_health(source: str) -> None:
    try:
        from monitoring.system_health_service import monitor

        monitor.ping_source(str(source or ""))
    except Exception:
        return None


class MarketIntelligenceService:
    """Assembles one intelligence snapshot per asset for the engine."""

    def __init__(self) -> None:
        self._sentiment_service = None
        self._sentiment_lock = threading.Lock()
        self._event_lock = threading.Lock()
        self._events: List[Dict[str, Any]] = []
        self._seen_signatures: Dict[str, datetime] = {}

    def get_sentiment_details(self, asset: str, category: str = "") -> Dict[str, Any]:
        try:
            service = self._get_sentiment_service()
            if service is None:
                return {"score": 0.0, "composite_score": 0.0, "components": {}, "weights": {}}
            result = service.get_comprehensive_sentiment(asset)
            if isinstance(result, dict):
                result.setdefault("components", {})
                result.setdefault("weights", {})
                return result
            score = float(result or 0.0)
            return {"score": score, "composite_score": score, "components": {}, "weights": {}}
        except Exception as exc:
            logger.warning(f"[MarketIntelligence] Sentiment fetch failed for {asset}: {exc}")
            return {"score": 0.0, "composite_score": 0.0, "components": {}, "weights": {}}

    def get_narrative_snapshot(self, asset: str) -> Dict[str, Any]:
        try:
            from narrative_ai import get_dominant_narrative, get_narrative_scores

            scores = get_narrative_scores()
            dominant = get_dominant_narrative()
            strength = max(scores.values()) if scores else 0.0
            return {
                "dominant_narrative": dominant,
                "narrative_strength": round(float(strength or 0.0), 3),
            }
        except Exception:
            return {"dominant_narrative": "", "narrative_strength": 0.0}

    def get_put_call_score(self, asset: str = "") -> Optional[float]:
        try:
            service = self._get_sentiment_service()
            if service is None:
                return None
            result = service.fetch_put_call_ratio()
            if result:
                return float(result.get("score", 0.0))
        except Exception:
            pass
        return None

    def get_reddit_sentiment_score(self, asset: str) -> Optional[float]:
        try:
            details = self.get_sentiment_details(asset)
            score = (details.get("components") or {}).get("reddit")
            if score is not None:
                return float(score)
        except Exception:
            pass
        try:
            service = self._get_sentiment_service()
            if service is None:
                return None
            result = service.get_reddit_sentiment_for_asset(asset)
            if result:
                return float(result.get("score", 0.0))
        except Exception:
            pass
        return None

    def get_free_market_intelligence(self, asset: str, category: str) -> Dict[str, Any]:
        try:
            from services.free_market_intelligence import free_market_intelligence

            result = free_market_intelligence.get_asset_context(asset, category)
            return result if isinstance(result, dict) else {}
        except Exception:
            return {}

    def get_derivatives_snapshot(self, asset: str) -> Dict[str, Any]:
        funding_bias = "NEUTRAL"
        oi_signal = "NEUTRAL"
        try:
            from data_ingestion import funding_monitor, oi_monitor

            symbol = asset.replace("-USD", "USDT").replace("/", "").replace("-", "")
            funding_bias = funding_monitor.get_bias(symbol)
            oi_signal = oi_monitor.get_signal(symbol)
        except Exception:
            pass
        return {
            "funding_bias": funding_bias,
            "oi_signal": oi_signal,
        }

    def get_whale_events(
        self,
        asset: str = "",
        *,
        min_value_usd: float = 500_000,
        hours: int = 24,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        max_age = timedelta(hours=max(1, int(hours or 24)))
        relevant = self.get_recent_events(
            asset=asset,
            event_types={"whale_alert", "onchain_event"},
            max_age=max_age,
        )
        formatted: List[Dict[str, Any]] = []
        threshold = float(min_value_usd or 0.0)
        for item in relevant:
            size_usd = float(item.get("size_usd", 0.0) or 0.0)
            if size_usd < threshold:
                continue
            payload = dict(item.get("payload") or {})
            ts = item.get("timestamp")
            asset_name = str(item.get("asset", ""))
            formatted.append({
                "event_id": item.get("event_id", ""),
                "event_type": item.get("event_type", ""),
                "asset": asset_name,
                "symbol": asset_name,
                "value_usd": round(size_usd, 2),
                "size_usd": round(size_usd, 2),
                "direction": item.get("direction", ""),
                "source": item.get("source", ""),
                "sentiment": float(item.get("sentiment", item.get("strength", 0.0)) or 0.0),
                "confidence": float(item.get("confidence", 0.0) or 0.0),
                "title": payload.get("title") or self._dashboard_event_title(item),
                "raw_text": item.get("raw_text", ""),
                "url": payload.get("url", ""),
                "subreddit": payload.get("subreddit", ""),
                "date": ts.isoformat() if isinstance(ts, datetime) else str(ts or ""),
                "alert_time": ts.isoformat() if isinstance(ts, datetime) else str(ts or ""),
                "timestamp": ts.isoformat() if isinstance(ts, datetime) else str(ts or ""),
            })
        return formatted[:limit]

    def get_whale_dashboard_summary(
        self,
        *,
        min_value_usd: float = 500_000,
        hours: int = 24,
        recent_limit: int = 10,
        alert_limit: int = 20,
    ) -> Dict[str, Any]:
        alerts = self.get_whale_events(
            min_value_usd=min_value_usd,
            hours=hours,
            limit=max(alert_limit, recent_limit, 50),
        )
        total_vol = sum(float(item.get("value_usd", 0.0) or 0.0) for item in alerts)
        by_asset: Dict[str, float] = {}
        for item in alerts:
            asset = str(item.get("asset", item.get("symbol", "")))
            if not asset:
                continue
            by_asset[asset] = by_asset.get(asset, 0.0) + float(item.get("value_usd", 0.0) or 0.0)
        top_assets = sorted(by_asset.items(), key=lambda entry: entry[1], reverse=True)[:8]
        return {
            "success": True,
            "alerts": alerts[:alert_limit],
            "total_volume_usd": round(total_vol, 0),
            "alert_count_24h": len(alerts),
            "top_assets": [{"asset": asset, "volume": round(volume)} for asset, volume in top_assets],
            "recent": alerts[:recent_limit],
        }

    def get_market_events(self, days: int = 7, limit: int = 10) -> Dict[str, Any]:
        try:
            from market_calendar import MarketCalendar

            calendar = MarketCalendar()
            events = calendar.get_high_impact_events(days=days)
            formatted = [self._format_market_event(event) for event in (events or [])[:limit]]
            return {
                "events": formatted,
                "earnings": [],
                "halving": calendar.get_halving_countdown(),
                "risk_outlook": calendar.should_reduce_risk(),
            }
        except Exception:
            return {"events": [], "earnings": [], "halving": {}, "risk_outlook": {}}

    def record_whale_alert(
        self,
        asset: str,
        direction: str,
        size_usd: float,
        source: str = "",
        sentiment: float = 0.1,
        timestamp: Any = None,
        raw_text: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        external_id: str = "",
    ) -> Dict[str, Any]:
        ts = _normalize_timestamp(timestamp)
        source_name = str(source or "whale_alert")
        signature = self._event_signature(
            event_type="whale_alert",
            asset=asset,
            direction=_normalize_direction(direction, sentiment),
            size_usd=float(size_usd or 0.0),
            source=source_name,
            timestamp=ts,
            raw_text=raw_text,
            external_id=external_id,
        )
        existing = self._get_existing_event(signature)
        if existing is not None:
            return existing
        event = {
            "event_id": f"whale:{asset}:{int(ts.timestamp() * 1000)}:{len(self._events)}",
            "asset": asset,
            "event_type": "whale_alert",
            "direction": _normalize_direction(direction, sentiment),
            "strength": round(min(1.0, abs(float(sentiment or 0.0))), 3),
            "confidence": round(self._source_confidence(source_name, float(size_usd or 0.0)), 3),
            "source": source_name,
            "timestamp": ts,
            "raw_text": raw_text,
            "payload": dict(metadata or {}),
            "size_usd": float(size_usd or 0.0),
            "sentiment": float(sentiment or 0.0),
            "signature": signature,
        }
        self._append_event(event, _WHALE_EVENT_TTL, signature)
        _ping_health("whale")
        return event

    def record_onchain_event(self, event: Dict[str, Any], external_id: str = "") -> Dict[str, Any]:
        payload = dict(event or {})
        ts = _normalize_timestamp(payload.get("ts") or payload.get("timestamp"))
        event_type = str(payload.get("type", "ONCHAIN_EVENT")).upper()
        asset = str(payload.get("asset", "")).strip()
        direction = "BUY"
        if any(tag in event_type for tag in ("DISTRIBUTION", "OUTFLOW", "SELL")):
            direction = "SELL"
        size_hint = float(payload.get("value_usd", 0.0) or 0.0)
        if not size_hint:
            delta = abs(float(payload.get("delta", 0.0) or 0.0))
            size_hint = delta * 1_000_000
        signature = self._event_signature(
            event_type="onchain_event",
            asset=asset,
            direction=direction,
            size_usd=size_hint,
            source=str(payload.get("source", "on-chain")),
            timestamp=ts,
            raw_text=str(payload.get("label", payload.get("type", ""))),
            external_id=external_id,
        )
        existing = self._get_existing_event(signature)
        if existing is not None:
            return existing
        normalized = {
            "event_id": f"onchain:{asset}:{int(ts.timestamp() * 1000)}:{len(self._events)}",
            "asset": asset,
            "event_type": "onchain_event",
            "direction": direction,
            "strength": round(min(1.0, abs(float(payload.get("delta", 0.0) or 0.0)) / 1000.0), 3),
            "confidence": round(self._source_confidence(str(payload.get("source", "on-chain")), size_hint), 3),
            "source": str(payload.get("source", "on-chain")),
            "timestamp": ts,
            "raw_text": str(payload.get("label", payload.get("type", ""))),
            "payload": payload,
            "onchain_type": event_type,
            "size_usd": size_hint,
            "signature": signature,
        }
        self._append_event(normalized, _ONCHAIN_EVENT_TTL, signature)
        return normalized

    def get_recent_events(
        self,
        asset: str = "",
        event_types: Optional[Iterable[str]] = None,
        max_age: Optional[timedelta] = None,
    ) -> List[Dict[str, Any]]:
        cutoff = _utc_now() - (max_age or _ONCHAIN_EVENT_TTL)
        kinds = {str(v).lower() for v in event_types} if event_types else None
        with self._event_lock:
            self._prune_locked()
            events = list(self._events)
        results: List[Dict[str, Any]] = []
        for entry in events:
            ts = entry.get("timestamp")
            if not isinstance(ts, datetime) or ts < cutoff:
                continue
            if kinds and str(entry.get("event_type", "")).lower() not in kinds:
                continue
            if asset and not _match_asset(asset, str(entry.get("asset", ""))):
                continue
            results.append(entry)
        results.sort(key=lambda item: item.get("timestamp", datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        return results

    def get_whale_snapshot(self, asset: str) -> Dict[str, Any]:
        relevant = self.get_recent_events(asset=asset, event_types={"whale_alert", "onchain_event"}, max_age=_ONCHAIN_EVENT_TTL)
        whale_events = [item for item in relevant if item.get("event_type") == "whale_alert"]
        onchain_events = [item for item in relevant if item.get("event_type") == "onchain_event"]

        min_whale_usd = 1_000_000
        buy_vol = sum(float(item.get("size_usd", 0.0) or 0.0) for item in whale_events if item.get("direction") == "BUY" and float(item.get("size_usd", 0.0) or 0.0) >= min_whale_usd)
        sell_vol = sum(float(item.get("size_usd", 0.0) or 0.0) for item in whale_events if item.get("direction") == "SELL" and float(item.get("size_usd", 0.0) or 0.0) >= min_whale_usd)
        total = buy_vol + sell_vol

        onchain_buys = sum(1 for item in onchain_events if item.get("direction") == "BUY")
        onchain_sells = sum(1 for item in onchain_events if item.get("direction") == "SELL")
        clusters = sum(1 for item in onchain_events if "CLUSTER" in str(item.get("onchain_type", "")))

        weighted_bull = 0.0
        weighted_bear = 0.0
        for item in whale_events:
            base_weight = self._source_weight(str(item.get("source", ""))) * self._size_weight(float(item.get("size_usd", 0.0) or 0.0))
            sent = float(item.get("sentiment", 0.0) or 0.0)
            if item.get("direction") == "BUY":
                weighted_bull += base_weight * max(0.0, sent if sent else float(item.get("strength", 0.1) or 0.1))
            else:
                weighted_bear += base_weight * max(0.0, abs(sent) if sent else float(item.get("strength", 0.1) or 0.1))

        if total > 0:
            bull_ratio = buy_vol / total
            weighted_bull += bull_ratio * 0.5
            weighted_bear += (1.0 - bull_ratio) * 0.5

        wtotal = weighted_bull + weighted_bear
        dominant = "BUY" if weighted_bull >= weighted_bear else "SELL"
        ratio = max(weighted_bull, weighted_bear) / wtotal if wtotal > 0 else 0.5

        if onchain_buys > onchain_sells:
            dominant = "BUY"
            ratio = min(1.0, ratio + 0.1)
        elif onchain_sells > onchain_buys:
            dominant = "SELL"
            ratio = min(1.0, ratio + 0.1)

        source_breakdown: Dict[str, int] = {}
        for item in whale_events:
            src = str(item.get("source", "unknown"))
            source_breakdown[src] = source_breakdown.get(src, 0) + 1

        if total == 0 and not onchain_events:
            return {
                "applicable": True,
                "has_data": False,
                "reason": "no_data",
                "buy_vol_m": 0.0,
                "sell_vol_m": 0.0,
                "dominant": None,
                "ratio": 0.5,
                "clusters": 0,
                "weighted_bull": 0.0,
                "weighted_bear": 0.0,
                "onchain_buys": 0,
                "onchain_sells": 0,
                "phase2": "no_recent_activity",
                "source_breakdown": {},
                "events": [],
            }

        return {
            "applicable": True,
            "has_data": True,
            "reason": "ok",
            "buy_vol_m": round(buy_vol / 1e6, 2),
            "sell_vol_m": round(sell_vol / 1e6, 2),
            "dominant": dominant,
            "ratio": round(ratio, 3),
            "clusters": clusters,
            "weighted_bull": round(weighted_bull, 3),
            "weighted_bear": round(weighted_bear, 3),
            "onchain_buys": onchain_buys,
            "onchain_sells": onchain_sells,
            "phase2": "whale_intelligence" if onchain_events else "whale_flow",
            "source_breakdown": source_breakdown,
            "events": relevant[:10],
        }

    def get_asset_snapshot(self, asset: str, category: str = "") -> Dict[str, Any]:
        sentiment_details = self.get_sentiment_details(asset, category)
        sentiment_score = float(sentiment_details.get("composite_score", sentiment_details.get("score", 0.0)) or 0.0)
        free_market_intelligence = self.get_free_market_intelligence(asset, category)
        market_intelligence_score = float(free_market_intelligence.get("score", 0.0) or 0.0)
        market_intelligence_sources = list(free_market_intelligence.get("sources") or [])
        market_intelligence_details = dict(free_market_intelligence.get("details") or {})
        derivatives = self.get_derivatives_snapshot(asset)
        narrative = self.get_narrative_snapshot(asset)
        whale_snapshot = self.get_whale_snapshot(asset)
        return {
            "asset": asset,
            "category": category,
            "sentiment_score": round(sentiment_score, 3),
            "sentiment_details": sentiment_details,
            "free_market_intelligence": free_market_intelligence,
            "market_intelligence_score": round(market_intelligence_score, 3),
            "market_intelligence_sources": market_intelligence_sources,
            "market_intelligence_details": market_intelligence_details,
            "funding_bias": derivatives.get("funding_bias", "NEUTRAL"),
            "oi_signal": derivatives.get("oi_signal", "NEUTRAL"),
            "dominant_narrative": narrative.get("dominant_narrative", ""),
            "narrative_strength": narrative.get("narrative_strength", 0.0),
            "whale_snapshot": whale_snapshot,
            "intelligence_timestamp": _utc_now().isoformat(),
        }

    def _get_sentiment_service(self):
        if self._sentiment_service is not None:
            return self._sentiment_service
        with self._sentiment_lock:
            if self._sentiment_service is None:
                try:
                    from services.sentiment_service import get_service

                    self._sentiment_service = get_service()
                except Exception:
                    self._sentiment_service = None
        return self._sentiment_service

    def _append_event(self, event: Dict[str, Any], ttl: timedelta, signature: str = "") -> None:
        with self._event_lock:
            self._events.append(event)
            if signature:
                self._seen_signatures[signature] = _utc_now() + ttl * 2
            self._prune_locked()
            cutoff = _utc_now() - ttl * 2
            self._events[:] = [item for item in self._events if isinstance(item.get("timestamp"), datetime) and item["timestamp"] > cutoff]

    def _prune_locked(self) -> None:
        cutoff = _utc_now() - _ONCHAIN_EVENT_TTL * 2
        self._events[:] = [item for item in self._events if isinstance(item.get("timestamp"), datetime) and item["timestamp"] > cutoff]
        now = _utc_now()
        self._seen_signatures = {
            key: expiry for key, expiry in self._seen_signatures.items()
            if expiry > now
        }

    def _get_existing_event(self, signature: str) -> Optional[Dict[str, Any]]:
        if not signature:
            return None
        with self._event_lock:
            self._prune_locked()
            expiry = self._seen_signatures.get(signature)
            if not expiry or expiry <= _utc_now():
                return None
            for item in reversed(self._events):
                if item.get("signature") == signature:
                    return item
        return None

    @staticmethod
    def _dashboard_event_title(item: Dict[str, Any]) -> str:
        payload = dict(item.get("payload") or {})
        if payload.get("title"):
            return str(payload["title"])
        raw_text = str(item.get("raw_text", "") or "").strip()
        if raw_text:
            return raw_text[:120]
        asset = str(item.get("asset", "") or "?")
        event_type = str(item.get("event_type", "") or "event").replace("_", " ").title()
        direction = str(item.get("direction", "") or "").upper()
        return f"{asset} {direction} {event_type}".strip()

    @staticmethod
    def _format_market_event(event: Dict[str, Any]) -> Dict[str, Any]:
        when = event.get("date")
        if isinstance(when, datetime):
            display_when = to_display_datetime(when)
            date_text = display_when.strftime("%Y-%m-%d") if display_when else when.strftime("%Y-%m-%d")
            time_text = f"{display_when.strftime('%H:%M')} {display_timezone_label()}" if display_when else ""
        else:
            raw = str(when or "")
            if " " in raw:
                date_text, time_text = raw.split(" ", 1)
            else:
                date_text, time_text = raw, ""
        title = event.get("title") or event.get("event") or event.get("name") or ""
        return {
            "title": str(title),
            "event": str(title),
            "date": date_text,
            "time": time_text,
            "impact": event.get("impact", ""),
            "forecast": event.get("forecast", event.get("estimate", "")),
            "previous": event.get("previous", event.get("actual", "")),
            "source": event.get("source", "Deriv"),
        }

    @staticmethod
    def _event_signature(
        *,
        event_type: str,
        asset: str,
        direction: str,
        size_usd: float,
        source: str,
        timestamp: datetime,
        raw_text: str = "",
        external_id: str = "",
    ) -> str:
        if external_id:
            return f"{event_type}:{external_id}"
        minute_bucket = timestamp.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        text_key = (raw_text or "").strip().lower()[:120]
        return "|".join([
            event_type,
            (asset or "").upper(),
            (direction or "").upper(),
            str(int(float(size_usd or 0.0))),
            (source or "").lower(),
            minute_bucket,
            text_key,
        ])

    @staticmethod
    def _source_weight(source: str) -> float:
        raw = str(source or "").lower()
        for key, weight in _SOURCE_CONFIDENCE.items():
            if key in raw:
                return weight
        return 0.60

    @classmethod
    def _source_confidence(cls, source: str, size_usd: float) -> float:
        return min(1.0, cls._source_weight(source) * (0.7 + min(0.3, float(size_usd or 0.0) / 100_000_000.0)))

    @staticmethod
    def _size_weight(size_usd: float) -> float:
        if size_usd >= 100_000_000:
            return 1.0
        if size_usd >= 10_000_000:
            return 0.75
        if size_usd >= 5_000_000:
            return 0.55
        return 0.35


_instance: Optional[MarketIntelligenceService] = None
_instance_lock = threading.Lock()


def get_service() -> MarketIntelligenceService:
    global _instance
    if _instance is not None:
        return _instance
    with _instance_lock:
        if _instance is None:
            _instance = MarketIntelligenceService()
    return _instance


__all__ = ["MarketIntelligenceService", "get_service"]
