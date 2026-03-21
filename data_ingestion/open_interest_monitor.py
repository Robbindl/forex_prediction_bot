from __future__ import annotations

import json
import threading
import time
from typing import Dict, List, Optional

import requests

from utils.logger import get_logger

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
OI_SPIKE_THRESHOLD_PCT = 3.0    # 3 % change in one poll period = notable
POLL_INTERVAL          = 300    # 5 minutes

TRACKED_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

BYBIT_OI_URL = "https://api.bybit.com/v5/market/open-interest"


class OpenInterestMonitor:
    """
    Tracks open interest per asset and publishes notable changes.
    Also exposes price-correlation helpers used by the meta-model.
    """

    def __init__(self, poll_interval_secs: int = POLL_INTERVAL) -> None:
        self._interval = poll_interval_secs
        self._running  = False
        self._prev_oi: Dict[str, float] = {}
        self._prices:  Dict[str, float] = {}    # kept up-to-date by ExchangeStreamManager
        self._history: Dict[str, List[dict]] = {}
        self._pub                        = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._init_redis()
        t = threading.Thread(target=self._loop, name="OIMonitor", daemon=True)
        t.start()
        logger.info(f"[OIMonitor] Started — tracking {len(TRACKED_SYMBOLS)} symbols")

    def stop(self) -> None:
        self._running = False

    def update_price(self, asset: str, price: float) -> None:
        """
        Called by ExchangeStreamManager handler so we can correlate OI with price.
        asset should be the exchange symbol, e.g. 'BTCUSDT'.
        """
        self._prices[asset] = price

    def get_oi(self, symbol: str) -> Optional[float]:
        return self._prev_oi.get(symbol)

    def get_signal(self, symbol: str) -> str:
        """
        Returns a quick signal label for the prediction pipeline.
        TREND_CONTINUATION | POTENTIAL_REVERSAL | NEUTRAL
        """
        hist = self._history.get(symbol, [])
        if len(hist) < 2:
            return "NEUTRAL"
        last_change = hist[-1].get("change_pct", 0)
        if abs(last_change) < OI_SPIKE_THRESHOLD_PCT:
            return "NEUTRAL"
        return hist[-1].get("signal", "NEUTRAL")

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.warning(f"[OIMonitor] Redis unavailable: {e}")

    def _loop(self) -> None:
        while self._running:
            for symbol in TRACKED_SYMBOLS:
                try:
                    oi = self._fetch_bybit(symbol)
                    if oi is not None:
                        self._analyse(symbol, oi)
                except Exception as e:
                    logger.debug(f"[OIMonitor] {symbol}: {e}")
                time.sleep(0.3)
            time.sleep(self._interval)

    def _fetch_bybit(self, symbol: str) -> Optional[float]:
        try:
            resp = requests.get(
                BYBIT_OI_URL,
                params={
                    "category":     "linear",
                    "symbol":       symbol,
                    "intervalTime": "5min",
                    "limit":        1,
                },
                timeout=10,
            )
            resp.raise_for_status()
            rows = resp.json().get("result", {}).get("list", [])
            if rows:
                return float(rows[0]["openInterest"])
        except Exception as e:
            logger.debug(f"[OIMonitor] Bybit OI {symbol}: {e}")
        return None

    def _analyse(self, symbol: str, oi: float) -> None:
        prev = self._prev_oi.get(symbol)
        if prev is None:
            self._prev_oi[symbol] = oi
            return

        change_pct = (oi - prev) / prev * 100 if prev else 0.0
        self._prev_oi[symbol] = oi

        if abs(change_pct) < OI_SPIKE_THRESHOLD_PCT:
            return

        signal = self._classify(symbol, change_pct)
        entry  = {
            "type":       "OI_CHANGE_ALERT",
            "asset":      symbol,
            "prev_oi":    round(prev, 2),
            "current_oi": round(oi, 2),
            "change_pct": round(change_pct, 3),
            "signal":     signal,
            "price":      self._prices.get(symbol, 0),
            "ts":         int(time.time() * 1000),
        }

        hist = self._history.setdefault(symbol, [])
        hist.append(entry)
        if len(hist) > 100:
            hist.pop(0)

        if self._pub:
            try:
                self._pub.publish("OI_CHANGE_ALERT", json.dumps(entry))
            except Exception as e:
                logger.debug(f"[OIMonitor] Redis publish: {e}")

        logger.info(f"[OIMonitor] {symbol} OI {change_pct:+.1f}% → [{signal}]")

    @staticmethod
    def _classify(symbol: str, oi_change_pct: float) -> str:
        """
        Simple OI signal classification.
        A full implementation would cross-reference price direction too.
        """
        if oi_change_pct > 0:
            # More contracts opened — market gaining conviction
            return "TREND_CONTINUATION"
        # Contracts closing — potential reversal / exhaustion
        return "POTENTIAL_REVERSAL"