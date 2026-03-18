from __future__ import annotations

import json
import threading
import time
from typing import Dict, List, Optional

import requests

from utils.logger import get_logger

logger = get_logger()

# ── Thresholds ────────────────────────────────────────────────────────────────
EXTREME_LONG_THRESHOLD  =  0.0100   # +1.00% per 8h (annualised ≈ 1095%)
EXTREME_SHORT_THRESHOLD = -0.0050   # −0.50% per 8h
HIGH_LONG_THRESHOLD     =  0.0050   # warn level
HIGH_SHORT_THRESHOLD    = -0.0025

TRACKED_SYMBOLS: List[str] = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT",
]

POLL_INTERVAL = 300   # 5 minutes

BYBIT_FUNDING_URL = "https://api.bybit.com/v5/market/funding/history"


class FundingRateMonitor:
    """
    Polls Bybit for current funding rates.
    Publishes FUNDING_RATE_ALERT when rates reach extreme levels.
    """

    def __init__(self, poll_interval_secs: int = POLL_INTERVAL) -> None:
        self._interval = poll_interval_secs
        self._running  = False
        self._history: Dict[str, List[dict]] = {}   # symbol → list of {rate, ts}
        self._pub                             = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._init_redis()
        t = threading.Thread(
            target=self._loop, name="FundingMonitor", daemon=True
        )
        t.start()
        logger.info(f"[FundingMonitor] Started — tracking {len(TRACKED_SYMBOLS)} symbols")

    def stop(self) -> None:
        self._running = False

    def get_rates(self) -> Dict[str, float]:
        """Return most recent rate for each tracked symbol."""
        result = {}
        for sym, hist in self._history.items():
            if hist:
                result[sym] = hist[-1]["rate"]
        return result

    def get_bias(self, symbol: str) -> str:
        """Returns: EXTREME_LONG | HIGH_LONG | NEUTRAL | HIGH_SHORT | EXTREME_SHORT"""
        hist = self._history.get(symbol, [])
        if not hist:
            return "NEUTRAL"
        rate = hist[-1]["rate"]
        if   rate >=  EXTREME_LONG_THRESHOLD:  return "EXTREME_LONG"
        elif rate >=  HIGH_LONG_THRESHOLD:      return "HIGH_LONG"
        elif rate <= EXTREME_SHORT_THRESHOLD:   return "EXTREME_SHORT"
        elif rate <= HIGH_SHORT_THRESHOLD:      return "HIGH_SHORT"
        return "NEUTRAL"

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[FundingMonitor] Redis unavailable: {e}")

    def _loop(self) -> None:
        while self._running:
            for symbol in TRACKED_SYMBOLS:
                try:
                    rate = self._fetch_bybit(symbol)
                    if rate is not None:
                        self._analyse(symbol, rate)
                except Exception as e:
                    logger.debug(f"[FundingMonitor] {symbol}: {e}")
                time.sleep(0.3)    # gentle rate limiting
            time.sleep(self._interval)

    def _fetch_bybit(self, symbol: str) -> Optional[float]:
        try:
            resp = requests.get(
                BYBIT_FUNDING_URL,
                params={"category": "linear", "symbol": symbol, "limit": 1},
                timeout=10,
            )
            resp.raise_for_status()
            rows = resp.json().get("result", {}).get("list", [])
            if rows:
                return float(rows[0]["fundingRate"])
        except Exception as e:
            logger.debug(f"[FundingMonitor] Bybit fetch {symbol}: {e}")
        return None

    def _analyse(self, symbol: str, rate: float) -> None:
        hist = self._history.setdefault(symbol, [])
        hist.append({"rate": rate, "ts": int(time.time() * 1000)})
        if len(hist) > 200:
            hist.pop(0)

        bias = self.get_bias(symbol)
        if bias == "NEUTRAL":
            return

        implication = {
            "EXTREME_LONG":  "Over-leveraged longs — long squeeze risk HIGH",
            "HIGH_LONG":     "Elevated longs — monitor for squeeze",
            "EXTREME_SHORT": "Over-leveraged shorts — short squeeze risk HIGH",
            "HIGH_SHORT":    "Elevated shorts — monitor for bounce",
        }.get(bias, "")

        event = {
            "type":        "FUNDING_RATE_ALERT",
            "asset":       symbol,
            "rate":        rate,
            "rate_pct":    round(rate * 100, 4),
            "bias":        bias,
            "implication": implication,
            "ts":          int(time.time() * 1000),
        }
        if self._pub:
            try:
                self._pub.publish("FUNDING_RATE_ALERT", json.dumps(event))
            except Exception as e:
                logger.debug(f"[FundingMonitor] Redis publish: {e}")

        level = "warning" if "EXTREME" in bias else "info"
        getattr(logger, level)(
            f"[FundingMonitor] {symbol} rate={rate:.4%} [{bias}] — {implication}"
        )