from __future__ import annotations

from typing import Any, Dict, List, Optional

from config.config import get_timeframe_periods
from core.assets import registry
from data.cache import Cache

_MOMENTUM_CACHE = Cache(default_ttl=20)

_NORMALIZATION_FLOORS = {
    "forex": 0.0025,
    "crypto": 0.0100,
    "commodities": 0.0040,
    "indices": 0.0035,
    "equities": 0.0040,
    "context": 0.0040,
}

_BINANCE_EQUITY_PROXY = {
    "category": "equities",
    "provider_preference": ("binance",),
}

_BINANCE_COMMODITY_PROXY = {
    "category": "commodities",
    "provider_preference": ("binance",),
}

_RELATIONSHIPS: Dict[str, List[Dict[str, Any]]] = {
    "EUR/USD": [
        {"peer": "GBP/USD", "mode": "same", "weight": 0.40, "label": "usd_leg_confirmation"},
        {"peer": "XAU/USD", "mode": "same", "weight": 0.25, "label": "usd_weakness_gold"},
        {"peer": "USD/CHF", "mode": "inverse", "weight": 0.20, "label": "swissy_usd_inverse"},
        {"peer": "US500", "mode": "same", "weight": 0.15, "label": "risk_on_usd_softness"},
    ],
    "GBP/USD": [
        {"peer": "EUR/USD", "mode": "same", "weight": 0.40, "label": "usd_leg_confirmation"},
        {"peer": "EUR/GBP", "mode": "inverse", "weight": 0.25, "label": "sterling_cross_confirmation"},
        {"peer": "UK100", "mode": "inverse", "weight": 0.20, "label": "ftse_gbp_inverse"},
        {"peer": "US500", "mode": "same", "weight": 0.15, "label": "risk_on_usd_softness"},
    ],
    "AUD/USD": [
        {"peer": "NZD/USD", "mode": "same", "weight": 0.35, "label": "antipodean_confirmation"},
        {"peer": "US500", "mode": "same", "weight": 0.30, "label": "risk_on_confirmation"},
        {"peer": "WTI", "mode": "same", "weight": 0.20, "label": "commodity_complex"},
        {"peer": "XAU/USD", "mode": "same", "weight": 0.15, "label": "commodity_beta"},
        {"peer": "XCU", "mode": "same", "weight": 0.18, "label": "copper_growth_proxy", **_BINANCE_COMMODITY_PROXY},
    ],
    "NZD/USD": [
        {"peer": "AUD/USD", "mode": "same", "weight": 0.40, "label": "antipodean_confirmation"},
        {"peer": "US500", "mode": "same", "weight": 0.25, "label": "risk_on_confirmation"},
        {"peer": "WTI", "mode": "same", "weight": 0.20, "label": "commodity_complex"},
        {"peer": "XAU/USD", "mode": "same", "weight": 0.15, "label": "commodity_beta"},
        {"peer": "XCU", "mode": "same", "weight": 0.15, "label": "copper_growth_proxy", **_BINANCE_COMMODITY_PROXY},
    ],
    "EUR/GBP": [
        {"peer": "EUR/USD", "mode": "same", "weight": 0.45, "label": "euro_strength"},
        {"peer": "GBP/USD", "mode": "inverse", "weight": 0.35, "label": "sterling_inverse"},
        {"peer": "UK100", "mode": "same", "weight": 0.20, "label": "ftse_sterling_spillover"},
    ],
    "USD/CAD": [
        {"peer": "WTI", "mode": "inverse", "weight": 1.00, "label": "oil_cad_link"},
    ],
    "USD/CHF": [
        {"peer": "EUR/USD", "mode": "inverse", "weight": 0.40, "label": "usd_leg_inverse"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.35, "label": "gold_safe_haven_inverse"},
        {"peer": "US500", "mode": "same", "weight": 0.25, "label": "risk_on_dollar_chf"},
    ],
    "WTI": [
        {"peer": "USD/CAD", "mode": "inverse", "weight": 0.70, "label": "cad_confirmation"},
        {"peer": "US500", "mode": "same", "weight": 0.30, "label": "growth_cycle_confirmation"},
        {"peer": "XCU", "mode": "same", "weight": 0.20, "label": "industrial_demand_proxy", **_BINANCE_COMMODITY_PROXY},
        {"peer": "NATGAS", "mode": "same", "weight": 0.15, "label": "energy_complex_proxy", **_BINANCE_COMMODITY_PROXY},
    ],
    "XAU/USD": [
        {"peer": "XAG/USD", "mode": "same", "weight": 0.40, "label": "silver_confirmation"},
        {"peer": "US500", "mode": "inverse", "weight": 0.35, "label": "risk_off_equities"},
        {"peer": "US100", "mode": "inverse", "weight": 0.25, "label": "risk_off_tech"},
    ],
    "XAG/USD": [
        {"peer": "XAU/USD", "mode": "same", "weight": 0.60, "label": "gold_lead"},
        {"peer": "US500", "mode": "inverse", "weight": 0.20, "label": "risk_off_equities"},
        {"peer": "WTI", "mode": "same", "weight": 0.20, "label": "commodity_complex"},
    ],
    "US500": [
        {"peer": "US100", "mode": "same", "weight": 0.40, "label": "tech_breadth"},
        {"peer": "US30", "mode": "same", "weight": 0.30, "label": "dow_breadth"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.30, "label": "gold_risk_off"},
        {"peer": "SPY", "mode": "same", "weight": 0.35, "label": "spy_etf_proxy", **_BINANCE_EQUITY_PROXY},
        {"peer": "QQQ", "mode": "same", "weight": 0.18, "label": "qqq_breadth_proxy", **_BINANCE_EQUITY_PROXY},
    ],
    "US100": [
        {"peer": "US500", "mode": "same", "weight": 0.45, "label": "broad_equity_confirmation"},
        {"peer": "US30", "mode": "same", "weight": 0.20, "label": "dow_confirmation"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.35, "label": "gold_risk_off"},
        {"peer": "QQQ", "mode": "same", "weight": 0.40, "label": "qqq_etf_proxy", **_BINANCE_EQUITY_PROXY},
        {"peer": "NVDA", "mode": "same", "weight": 0.20, "label": "ai_leader_proxy", **_BINANCE_EQUITY_PROXY},
    ],
    "US30": [
        {"peer": "US500", "mode": "same", "weight": 0.50, "label": "broad_equity_confirmation"},
        {"peer": "US100", "mode": "same", "weight": 0.20, "label": "tech_confirmation"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.30, "label": "gold_risk_off"},
    ],
    "UK100": [
        {"peer": "GER40", "mode": "same", "weight": 0.30, "label": "europe_equity_confirmation"},
        {"peer": "US500", "mode": "same", "weight": 0.45, "label": "global_equity_confirmation"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.25, "label": "gold_risk_off"},
        {"peer": "WTI", "mode": "same", "weight": 0.30, "label": "energy_complex"},
    ],
    "GER40": [
        {"peer": "US500", "mode": "same", "weight": 0.40, "label": "global_equity_confirmation"},
        {"peer": "US100", "mode": "same", "weight": 0.25, "label": "tech_beta_confirmation"},
        {"peer": "UK100", "mode": "same", "weight": 0.20, "label": "europe_equity_confirmation"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.15, "label": "gold_risk_off"},
    ],
    "AUS200": [
        {"peer": "US500", "mode": "same", "weight": 0.35, "label": "global_equity_confirmation"},
        {"peer": "JPN225", "mode": "same", "weight": 0.25, "label": "asia_equity_confirmation"},
        {"peer": "WTI", "mode": "same", "weight": 0.20, "label": "commodity_complex"},
        {"peer": "AUD/USD", "mode": "same", "weight": 0.20, "label": "aussie_domestic_beta"},
    ],
    "JPN225": [
        {"peer": "US500", "mode": "same", "weight": 0.35, "label": "global_equity_confirmation"},
        {"peer": "US100", "mode": "same", "weight": 0.25, "label": "tech_beta_confirmation"},
        {"peer": "USD/JPY", "mode": "same", "weight": 0.25, "label": "yen_exporter_link"},
        {"peer": "AUS200", "mode": "same", "weight": 0.15, "label": "asia_equity_confirmation"},
        {"peer": "EWJ", "mode": "same", "weight": 0.30, "label": "japan_etf_proxy", **_BINANCE_EQUITY_PROXY},
    ],
    "USD/JPY": [
        {"peer": "US500", "mode": "same", "weight": 0.60, "label": "risk_on_yen"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.40, "label": "gold_risk_off"},
    ],
    "EUR/JPY": [
        {"peer": "US500", "mode": "same", "weight": 0.55, "label": "risk_on_yen"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.45, "label": "gold_risk_off"},
    ],
    "GBP/JPY": [
        {"peer": "US500", "mode": "same", "weight": 0.55, "label": "risk_on_yen"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.45, "label": "gold_risk_off"},
    ],
    "BTC-USD": [
        {"peer": "ETH-USD", "mode": "same", "weight": 0.45, "label": "crypto_breadth"},
        {"peer": "US100", "mode": "same", "weight": 0.25, "label": "risk_on_beta"},
        {"peer": "XAU/USD", "mode": "inverse", "weight": 0.30, "label": "gold_risk_off"},
        {"peer": "QQQ", "mode": "same", "weight": 0.20, "label": "nasdaq_proxy", **_BINANCE_EQUITY_PROXY},
        {"peer": "NVDA", "mode": "same", "weight": 0.15, "label": "ai_beta_proxy", **_BINANCE_EQUITY_PROXY},
    ],
    "ETH-USD": [
        {"peer": "BTC-USD", "mode": "same", "weight": 0.60, "label": "btc_lead"},
        {"peer": "BNB-USD", "mode": "same", "weight": 0.20, "label": "alt_breadth"},
        {"peer": "SOL-USD", "mode": "same", "weight": 0.20, "label": "alt_breadth"},
        {"peer": "QQQ", "mode": "same", "weight": 0.15, "label": "nasdaq_proxy", **_BINANCE_EQUITY_PROXY},
    ],
    "BNB-USD": [
        {"peer": "BTC-USD", "mode": "same", "weight": 0.55, "label": "btc_lead"},
        {"peer": "ETH-USD", "mode": "same", "weight": 0.45, "label": "eth_confirmation"},
    ],
    "SOL-USD": [
        {"peer": "BTC-USD", "mode": "same", "weight": 0.55, "label": "btc_lead"},
        {"peer": "ETH-USD", "mode": "same", "weight": 0.45, "label": "eth_confirmation"},
    ],
    "XRP-USD": [
        {"peer": "BTC-USD", "mode": "same", "weight": 0.60, "label": "btc_lead"},
        {"peer": "ETH-USD", "mode": "same", "weight": 0.40, "label": "crypto_breadth"},
    ],
}


def _clip(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(value or 0.0)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except Exception:
        return default


class CrossAssetSpilloverService:
    def _peer_momentum(
        self,
        fetcher: Any,
        asset: str,
        timeframe: str,
        *,
        category_override: Optional[str] = None,
        provider_preference: Optional[tuple[str, ...]] = None,
    ) -> Optional[Dict[str, Any]]:
        category = str(category_override or registry.category(asset) or "unknown").strip().lower()
        preference_key = ",".join(str(item or "").strip().lower() for item in tuple(provider_preference or ()) if str(item or "").strip()) or "default"
        cache_key = f"cross-asset:{asset}:{category}:{timeframe}:{preference_key}"
        cached = _MOMENTUM_CACHE.get(cache_key)
        if cached is not None:
            return dict(cached)

        if fetcher is None:
            return None

        periods = max(18, min(48, int(get_timeframe_periods(timeframe) or 24)))
        try:
            df = fetcher.get_ohlcv(
                asset,
                category,
                interval=timeframe,
                periods=periods,
                provider_preference=provider_preference,
            )
        except Exception:
            return None
        if df is None or getattr(df, "empty", True) or len(df) < 6:
            return None

        try:
            close = df["close"].astype(float)
            high = df["high"].astype(float)
            low = df["low"].astype(float)
        except Exception:
            return None

        last_close = _safe_float(close.iloc[-1], 0.0)
        if last_close <= 0.0:
            return None

        anchor_index = max(0, len(close) - 5)
        anchor_close = _safe_float(close.iloc[anchor_index], last_close)
        if anchor_close <= 0.0:
            anchor_close = last_close

        move_pct = (last_close - anchor_close) / anchor_close if anchor_close > 0 else 0.0
        range_pct = ((high - low) / close.replace(0, float("nan"))).dropna()
        avg_range_pct = _safe_float(range_pct.tail(8).mean(), 0.0)
        normalization = max(
            avg_range_pct * 2.5,
            _NORMALIZATION_FLOORS.get(category, 0.0030),
        )
        score = _clip(move_pct / normalization)

        if score >= 0.10:
            direction = "BUY"
        elif score <= -0.10:
            direction = "SELL"
        else:
            direction = "NEUTRAL"

        payload = {
            "asset": asset,
            "category": category,
            "score": round(score, 4),
            "move_pct": round(move_pct, 6),
            "avg_range_pct": round(avg_range_pct, 6),
            "direction": direction,
            "timeframe": timeframe,
            "bars": int(len(df)),
        }
        _MOMENTUM_CACHE.set(cache_key, dict(payload))
        return payload

    def build_snapshot(
        self,
        *,
        asset: str,
        category: str,
        fetcher: Any,
        timeframe: str = "15m",
    ) -> Dict[str, Any]:
        relations = list(_RELATIONSHIPS.get(str(asset or "").strip(), []))
        if not relations or fetcher is None:
            return {}

        peers: List[Dict[str, Any]] = []
        weighted_total = 0.0
        weight_sum = 0.0
        confidence_total = 0.0

        for relation in relations:
            peer_asset = str(relation.get("peer") or "").strip()
            if not peer_asset:
                continue
            relation_category = str(relation.get("category") or registry.category(peer_asset) or "unknown").strip().lower()
            relation_provider_preference = tuple(relation.get("provider_preference") or ())
            peer = self._peer_momentum(
                fetcher,
                peer_asset,
                timeframe,
                category_override=relation_category,
                provider_preference=relation_provider_preference,
            )
            if not peer:
                continue

            relation_sign = 1.0 if str(relation.get("mode") or "same").lower() == "same" else -1.0
            weight = max(0.0, _safe_float(relation.get("weight"), 0.0))
            buy_bias = _clip(peer["score"] * relation_sign)
            weighted_alignment = buy_bias * weight
            supportive_direction = "BUY" if buy_bias >= 0.0 else "SELL"

            if abs(buy_bias) >= 0.22:
                state = "supportive"
            elif abs(buy_bias) <= 0.08:
                state = "neutral"
            else:
                state = "mixed"

            peers.append(
                {
                    "peer_asset": peer_asset,
                    "peer_category": peer.get("category", relation_category),
                    "peer_score": round(float(peer["score"]), 4),
                    "peer_direction": str(peer.get("direction") or "NEUTRAL"),
                    "peer_move_pct": round(float(peer.get("move_pct", 0.0) or 0.0), 6),
                    "relation_mode": str(relation.get("mode") or "same"),
                    "relation_label": str(relation.get("label") or ""),
                    "weight": round(weight, 4),
                    "buy_bias": round(buy_bias, 4),
                    "weighted_alignment": round(weighted_alignment, 4),
                    "supportive_direction": supportive_direction,
                    "state": state,
                }
            )
            weighted_total += weighted_alignment
            weight_sum += weight
            confidence_total += abs(buy_bias) * weight

        if not peers or weight_sum <= 0.0:
            return {}

        score = _clip(weighted_total / weight_sum)
        confidence = max(0.0, min(1.0, confidence_total / weight_sum))
        peers.sort(key=lambda item: abs(float(item.get("weighted_alignment", 0.0) or 0.0)), reverse=True)
        dominant = peers[0]

        if score >= 0.22:
            state = "buy_support"
            notes = ["cross_asset_buy_support"]
        elif score <= -0.22:
            state = "sell_support"
            notes = ["cross_asset_sell_support"]
        else:
            state = "mixed"
            notes = ["cross_asset_mixed"]

        if confidence >= 0.70:
            notes.append("cross_asset_high_confidence")
        elif confidence <= 0.25:
            notes.append("cross_asset_low_confidence")

        return {
            "asset": asset,
            "category": category,
            "score": round(score, 4),
            "confidence": round(confidence, 4),
            "state": state,
            "supportive_direction": "BUY" if score >= 0.0 else "SELL",
            "dominant_peer": dominant.get("peer_asset", ""),
            "dominant_relation": dominant.get("relation_label", ""),
            "dominant_peer_direction": dominant.get("peer_direction", "NEUTRAL"),
            "peers": peers,
            "notes": notes,
            "timeframe": timeframe,
        }


_service = CrossAssetSpilloverService()


def get_service() -> CrossAssetSpilloverService:
    return _service
