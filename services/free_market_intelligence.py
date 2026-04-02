from __future__ import annotations

import io
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from config.config import (
    CFTC_ENABLED,
    EIA_API_KEY,
    EIA_CRUDE_STOCKS_SERIES,
    FRED_API_KEY,
    FRED_USD_BROAD_SERIES,
    FRED_US_10Y_SERIES,
    FRED_US_2Y_SERIES,
    FRED_US_REAL_10Y_SERIES,
    FRED_VIX_SERIES,
    FREE_INTEL_CACHE_SECONDS,
    FREE_INTEL_ENABLED,
)
from utils.logger import get_logger

logger = get_logger()

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
_EIA_URL = "https://api.eia.gov/v2/seriesid"
_CFTC_FINANCIAL_URL = "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip"
_CFTC_DISAGG_URL = "https://www.cftc.gov/files/dea/history/com_disagg_txt_{year}.zip"

_FINANCIAL_PATTERNS = {
    "EUR/USD": ["EURO FX - CHICAGO MERCANTILE EXCHANGE"],
    "GBP/USD": ["BRITISH POUND - CHICAGO MERCANTILE EXCHANGE"],
    "AUD/USD": ["AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"],
    "USD/CAD": ["CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE"],
    "USD/JPY": ["JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE"],
    "GBP/JPY": [
        "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE",
        "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE",
    ],
    "US500": ["E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"],
    "^GSPC": ["E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE"],
    "US100": [
        "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE",
        "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
        "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE",
    ],
    "^IXIC": [
        "NASDAQ-100 Consolidated - CHICAGO MERCANTILE EXCHANGE",
        "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
        "MICRO E-MINI NASDAQ-100 INDEX - CHICAGO MERCANTILE EXCHANGE",
    ],
    "US30": [
        "DJIA Consolidated - CHICAGO BOARD OF TRADE",
        "DJIA x $5 - CHICAGO BOARD OF TRADE",
    ],
    "^DJI": [
        "DJIA Consolidated - CHICAGO BOARD OF TRADE",
        "DJIA x $5 - CHICAGO BOARD OF TRADE",
    ],
}

_DISAGG_PATTERNS = {
    "XAU/USD": ["GOLD - COMMODITY EXCHANGE INC."],
    "GC=F": ["GOLD - COMMODITY EXCHANGE INC."],
    "XAG/USD": ["SILVER - COMMODITY EXCHANGE INC."],
    "SI=F": ["SILVER - COMMODITY EXCHANGE INC."],
    "WTI": [
        "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
        "CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE",
    ],
    "CL=F": [
        "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
        "CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE",
    ],
}


def _clamp(value: float, limit: float = 1.0) -> float:
    return max(-limit, min(limit, value))


@dataclass
class _CacheEntry:
    payload: Dict[str, Any]
    expires_at: float


class FreeMarketIntelligence:
    """
    Official free-data enrichment for non-crypto assets.

    Sources:
      - FRED for US rates / dollar / volatility proxies
      - EIA for WTI crude stocks
      - CFTC COT for positioning

    The output is a small, cached directional intelligence bundle that can be
    blended into the existing sentiment and governance layers.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, _CacheEntry] = {}
        self._lock = threading.Lock()
        self._frame_cache: Dict[str, _CacheEntry] = {}
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "Robbie-TradingBot/1.0"})

    def get_asset_context(self, asset: str, category: str, as_of: Any = None) -> Dict[str, Any]:
        if not FREE_INTEL_ENABLED:
            return self._empty(asset, category)

        as_of_dt = self._normalize_as_of(as_of)
        as_of_key = as_of_dt.isoformat() if as_of_dt is not None else "latest"
        cache_key = f"{asset}:{category}:{as_of_key}"
        now = time.time()
        with self._lock:
            hit = self._cache.get(cache_key)
            if hit and now < hit.expires_at:
                return dict(hit.payload)

        payload = self._build_asset_context(asset, category, as_of=as_of_dt)
        with self._lock:
            self._cache[cache_key] = _CacheEntry(
                payload=dict(payload),
                expires_at=now + max(60, FREE_INTEL_CACHE_SECONDS),
            )
        return payload

    def _build_asset_context(self, asset: str, category: str, as_of: Optional[datetime] = None) -> Dict[str, Any]:
        components: Dict[str, float] = {}
        details: Dict[str, Any] = {}
        sources: List[str] = []

        try:
            macro = self._macro_context(asset, category, as_of=as_of)
        except TypeError:
            macro = self._macro_context(asset, category)
        if macro:
            components.update(macro.get("components", {}))
            details["macro"] = macro.get("details", {})
            sources.extend(macro.get("sources", []))

        try:
            cot = self._cftc_context(asset, as_of=as_of)
        except TypeError:
            cot = self._cftc_context(asset)
        if cot:
            components["cftc_positioning"] = cot["score"]
            details["cftc"] = cot
            sources.append("cftc")

        if asset in {"WTI", "WTI/USD", "CL=F"}:
            try:
                eia = self._eia_context(as_of=as_of)
            except TypeError:
                eia = self._eia_context()
            if eia:
                components["eia_inventory"] = eia["score"]
                details["eia"] = eia
                sources.append("eia")

        if not components:
            return self._empty(asset, category)

        score = round(sum(components.values()) / max(1, len(components)), 3)
        payload = {
            "asset": asset,
            "category": category,
            "score": score,
            "components": {k: round(v, 3) for k, v in components.items()},
            "sources": sorted(set(sources)),
            "details": details,
            "timestamp": datetime.utcnow().isoformat(),
            "as_of": as_of.isoformat() if as_of is not None else None,
        }
        return payload

    @staticmethod
    def _empty(asset: str, category: str) -> Dict[str, Any]:
        return {
            "asset": asset,
            "category": category,
            "score": 0.0,
            "components": {},
            "sources": [],
            "details": {},
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _macro_context(self, asset: str, category: str, as_of: Optional[datetime] = None) -> Dict[str, Any]:
        usd_broad = self._fred_latest_change(FRED_USD_BROAD_SERIES, as_of=as_of)
        us2y = self._fred_latest_change(FRED_US_2Y_SERIES, as_of=as_of)
        us10y = self._fred_latest_change(FRED_US_10Y_SERIES, as_of=as_of)
        real10y = self._fred_latest_change(FRED_US_REAL_10Y_SERIES, as_of=as_of)
        vix = self._fred_latest_change(FRED_VIX_SERIES, as_of=as_of)

        details = {
            "usd_broad": usd_broad,
            "us2y": us2y,
            "us10y": us10y,
            "real10y": real10y,
            "vix": vix,
        }
        components: Dict[str, float] = {}

        usd_delta = float((usd_broad or {}).get("delta_pct", 0.0) or 0.0)
        us2y_delta = float((us2y or {}).get("delta", 0.0) or 0.0)
        curve = 0.0
        if us10y and us2y and us10y.get("latest") is not None and us2y.get("latest") is not None:
            curve = float(us10y["latest"]) - float(us2y["latest"])
        real_yield = float((real10y or {}).get("latest", 0.0) or 0.0)
        vix_level = float((vix or {}).get("latest", 0.0) or 0.0)
        vix_delta = float((vix or {}).get("delta_pct", 0.0) or 0.0)

        if category == "forex":
            usd_strength = _clamp(usd_delta * 6 + us2y_delta / 10 - vix_delta * 2)
            risk_on = _clamp((20.0 - vix_level) / 20.0)

            if asset in {"EUR/USD", "GBP/USD", "AUD/USD"}:
                components["usd_macro"] = -usd_strength
            elif asset in {"USD/CAD", "USD/JPY"}:
                components["usd_macro"] = usd_strength
            elif asset == "GBP/JPY":
                components["risk_regime"] = _clamp(risk_on - usd_strength * 0.3)

        elif category == "commodities":
            if asset in {"XAU/USD", "GC=F", "XAG/USD", "SI=F"}:
                components["real_yield"] = _clamp(-real_yield / 2.5)
                components["usd_macro"] = _clamp(-usd_delta * 6)
                components["risk_regime"] = _clamp((vix_level - 20.0) / 20.0 * 0.6)
            elif asset in {"WTI", "WTI/USD", "CL=F"}:
                components["usd_macro"] = _clamp(-usd_delta * 6)
                components["risk_regime"] = _clamp((20.0 - vix_level) / 18.0)

        elif category == "indices":
            components["risk_regime"] = _clamp((20.0 - vix_level) / 15.0 - vix_delta * 2.0)
            components["yield_curve"] = _clamp(curve / 2.0)
            components["real_yield"] = _clamp(-real_yield / 3.0)

        return {
            "components": components,
            "details": details,
            "sources": ["fred"],
        }

    def _fred_latest_change(self, series_id: str, as_of: Optional[datetime] = None) -> Optional[Dict[str, float]]:
        if not FRED_API_KEY or not series_id:
            return None
        try:
            observation_end = (as_of or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
            response = self._session.get(
                _FRED_URL,
                params={
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 8,
                    "observation_end": observation_end,
                },
                timeout=12,
            )
            response.raise_for_status()
            observations = response.json().get("observations", [])
            values: List[float] = []
            for item in observations:
                value = item.get("value")
                if value in (None, ".", ""):
                    continue
                try:
                    values.append(float(value))
                except Exception:
                    continue
                if len(values) >= 2:
                    break
            if len(values) < 2:
                return None
            latest, previous = values[0], values[1]
            delta = latest - previous
            delta_pct = delta / abs(previous) if previous else 0.0
            return {
                "latest": latest,
                "previous": previous,
                "delta": delta,
                "delta_pct": delta_pct,
            }
        except Exception as exc:
            logger.debug(f"[FreeIntel] FRED {series_id}: {exc}")
            return None

    def _eia_context(self, as_of: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        if not EIA_API_KEY:
            return None
        try:
            response = self._session.get(
                f"{_EIA_URL}/{EIA_CRUDE_STOCKS_SERIES}",
                params={"api_key": EIA_API_KEY},
                timeout=12,
            )
            response.raise_for_status()
            rows = (((response.json() or {}).get("response") or {}).get("data") or [])
            if as_of is not None:
                filtered = []
                for item in rows:
                    period = str(item.get("period", "") or "").strip()
                    try:
                        period_ts = pd.Timestamp(period)
                        period_ts = period_ts.tz_localize("UTC") if period_ts.tzinfo is None else period_ts.tz_convert("UTC")
                        if period_ts <= as_of:
                            filtered.append(item)
                    except Exception:
                        filtered.append(item)
                rows = filtered
            values: List[float] = []
            periods: List[str] = []
            for item in rows:
                raw = item.get("value")
                if raw in (None, ".", ""):
                    continue
                try:
                    values.append(float(raw))
                    periods.append(str(item.get("period", "")))
                except Exception:
                    continue
                if len(values) >= 2:
                    break
            if len(values) < 2:
                return None
            latest, previous = values[0], values[1]
            delta_pct = (latest - previous) / abs(previous) if previous else 0.0
            # Falling stocks are bullish for oil.
            score = _clamp(-delta_pct * 8)
            return {
                "series": EIA_CRUDE_STOCKS_SERIES,
                "period": periods[0] if periods else "",
                "latest": latest,
                "previous": previous,
                "delta_pct": delta_pct,
                "score": round(score, 3),
            }
        except Exception as exc:
            logger.debug(f"[FreeIntel] EIA {EIA_CRUDE_STOCKS_SERIES}: {exc}")
            return None

    def _cftc_context(self, asset: str, as_of: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        if not CFTC_ENABLED:
            return None

        if asset in _FINANCIAL_PATTERNS:
            report_type = "financial"
            frame = self._load_cftc_frame(_CFTC_FINANCIAL_URL)
            if frame is None:
                return None
            patterns = _FINANCIAL_PATTERNS[asset]
            matches = frame[frame["Market_and_Exchange_Names"].isin(patterns)]
            if matches.empty:
                return None

            if asset == "GBP/JPY":
                gbp = self._financial_position(matches, "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE", as_of=as_of)
                jpy = self._financial_position(matches, "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE", as_of=as_of)
                if gbp is None or jpy is None:
                    return None
                score = _clamp(gbp["score"] - jpy["score"])
                return {
                    "report_type": report_type,
                    "asset": asset,
                    "score": round(score, 3),
                    "legs": {"gbp": gbp, "jpy": jpy},
                }

            row_name = patterns[0]
            return self._financial_position(matches, row_name, asset=asset, as_of=as_of)

        if asset in _DISAGG_PATTERNS:
            report_type = "disaggregated"
            frame = self._load_cftc_frame(_CFTC_DISAGG_URL)
            if frame is None:
                return None
            patterns = _DISAGG_PATTERNS[asset]
            matches = frame[frame["Market_and_Exchange_Names"].isin(patterns)]
            if matches.empty:
                return None
            row = self._latest_cftc_row(matches, as_of=as_of)
            if row is None:
                return None
            open_interest = float(row.get("Open_Interest_All", 0) or 0)
            if open_interest <= 0:
                return None
            money_long = float(row.get("M_Money_Positions_Long_All", 0) or 0)
            money_short = float(row.get("M_Money_Positions_Short_All", 0) or 0)
            score = _clamp((money_long - money_short) / open_interest * 8)
            return {
                "report_type": report_type,
                "asset": asset,
                "market": str(row.get("Market_and_Exchange_Names", "")),
                "report_date": str(row.get("Report_Date_as_YYYY-MM-DD", "")),
                "score": round(score, 3),
                "managed_money_net": round(money_long - money_short, 2),
                "open_interest": round(open_interest, 2),
            }

        return None

    def _financial_position(
        self,
        frame: pd.DataFrame,
        market_name: str,
        asset: str = "",
        as_of: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        match = frame[frame["Market_and_Exchange_Names"] == market_name]
        if match.empty:
            return None
        row = self._latest_cftc_row(match, as_of=as_of)
        if row is None:
            return None
        open_interest = float(row.get("Open_Interest_All", 0) or 0)
        if open_interest <= 0:
            return None

        asset_mgr_net = float(row.get("Asset_Mgr_Positions_Long_All", 0) or 0) - float(
            row.get("Asset_Mgr_Positions_Short_All", 0) or 0
        )
        lev_money_net = float(row.get("Lev_Money_Positions_Long_All", 0) or 0) - float(
            row.get("Lev_Money_Positions_Short_All", 0) or 0
        )
        raw_score = ((asset_mgr_net / open_interest) * 4) + ((lev_money_net / open_interest) * 4)

        # USD-base pairs need inversion because the futures leg is the quote currency.
        if asset in {"USD/CAD", "USD/JPY"}:
            raw_score *= -1.0

        return {
            "report_type": "financial",
            "asset": asset,
            "market": str(row.get("Market_and_Exchange_Names", "")),
            "report_date": str(row.get("Report_Date_as_YYYY-MM-DD", "")),
            "score": round(_clamp(raw_score), 3),
            "asset_manager_net": round(asset_mgr_net, 2),
            "leveraged_money_net": round(lev_money_net, 2),
            "open_interest": round(open_interest, 2),
        }

    @staticmethod
    def _normalize_as_of(as_of: Any) -> Optional[datetime]:
        if as_of in (None, ""):
            return None
        try:
            if isinstance(as_of, datetime):
                return as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
            parsed = pd.Timestamp(as_of)
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            else:
                parsed = parsed.tz_convert("UTC")
            return parsed.to_pydatetime()
        except Exception:
            return None

    @staticmethod
    def _latest_cftc_row(frame: pd.DataFrame, as_of: Optional[datetime] = None):
        if frame is None or frame.empty:
            return None
        ordered = frame.copy()
        if as_of is not None and "Report_Date_as_YYYY-MM-DD" in ordered.columns:
            report_dates = pd.to_datetime(ordered["Report_Date_as_YYYY-MM-DD"], utc=True, errors="coerce")
            ordered = ordered.loc[report_dates <= as_of]
            if ordered.empty:
                return None
        ordered = ordered.sort_values("Report_Date_as_YYYY-MM-DD", ascending=False)
        return ordered.iloc[0]

    def _load_cftc_frame(self, url_template: str) -> Optional[pd.DataFrame]:
        year = datetime.utcnow().year
        for candidate_year in (year, year - 1):
            url = url_template.format(year=candidate_year)
            now = time.time()
            with self._lock:
                hit = self._frame_cache.get(url)
                if hit and now < hit.expires_at:
                    frame = hit.payload.get("frame")
                    if isinstance(frame, pd.DataFrame):
                        return frame.copy()
            try:
                response = self._session.get(url, timeout=20)
                response.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
                    with archive.open(archive.namelist()[0]) as handle:
                        frame = pd.read_csv(handle)
                        with self._lock:
                            self._frame_cache[url] = _CacheEntry(
                                payload={"frame": frame},
                                expires_at=now + max(900, FREE_INTEL_CACHE_SECONDS),
                            )
                        return frame.copy()
            except Exception as exc:
                logger.debug(f"[FreeIntel] CFTC {candidate_year}: {exc}")
                continue
        return None


free_market_intelligence = FreeMarketIntelligence()
