from __future__ import annotations

import json
import time
import threading
from collections import defaultdict
from typing import Dict, List, Optional

from utils.logger import get_logger

logger = get_logger()

# ── Narrative keyword registry ────────────────────────────────────────────────
NARRATIVE_KEYWORDS: Dict[str, List[str]] = {
    "AI_TOKENS": [
        "ai", "artificial intelligence", "gpt", "llm", "large language",
        "neural", "machine learning", "deep learning", "agent", "copilot",
        "worldcoin", "fetch.ai", "ocean protocol", "singularitynet",
    ],
    "ETF_NEWS": [
        "etf", "spot etf", "bitcoin etf", "ethereum etf",
        "sec approval", "sec rejection", "blackrock", "fidelity",
        "grayscale", "asset management", "institutional",
    ],
    "MACRO_SHOCK": [
        "recession", "inflation", "fomc", "federal reserve", "interest rate",
        "cpi", "ppi", "nfp", "jobs report", "gdp", "yield curve",
        "treasury", "dollar index", "dxy", "rate hike", "rate cut",
    ],
    "DEFI_TREND": [
        "defi", "yield farming", "liquidity pool", "tvl", "total value locked",
        "uniswap", "aave", "compound", "curve", "protocol", "vault",
        "impermanent loss", "apr", "apy",
    ],
    "REGULATION": [
        "sec", "regulation", "regulatory", "ban", "crackdown", "compliance",
        "kyc", "aml", "cftc", "finra", "enforcement", "lawsuit", "subpoena",
        "illegal", "sanctioned", "ofac",
    ],
    "LAYER2_TREND": [
        "layer2", "l2", "rollup", "arbitrum", "optimism", "zksync",
        "polygon", "starknet", "zkevm", "base", "scaling", "gas fees",
        "ethereum scaling",
    ],
    "BTC_DOMINANCE": [
        "bitcoin dominance", "btc dominance", "altcoin season", "alt season",
        "btc season", "rotation", "dominance chart", "market cap",
        "bitcoin market cap",
    ],
    "EXCHANGE_NEWS": [
        "exchange hack", "exchange exploit", "listing", "delisting",
        "coinbase listing", "binance listing", "withdrawal suspended",
        "exchange insolvent", "rug pull", "exit scam",
    ],
    "STABLECOIN_NEWS": [
        "depeg", "usdt", "usdc", "stablecoin", "tether", "peg broken",
        "stablecoin collapse", "algorithmic stablecoin", "reserve",
        "circle", "backing",
    ],
    "HALVING_BUZZ": [
        "halving", "half", "block reward", "miner reward", "supply shock",
        "post halving", "pre halving", "halving cycle",
    ],
}

# ── Configuration ─────────────────────────────────────────────────────────────
SNAPSHOT_INTERVAL    = 20    # compare counts every N ingestions
VELOCITY_THRESHOLD   = 0.40  # 40% increase in velocity = trend
MIN_COUNT_TO_ALERT   = 3     # need at least 3 mentions before alerting
ALERT_COOLDOWN_SECS  = 300   # one alert per narrative per 5 minutes


class TopicClusterEngine:
    """
    Stateful narrative tracker. Thread-safe — ingest() may be called
    from NewsAnalyzer, RedditTopicTracker, and TwitterTopicTracker
    concurrently.
    """

    def __init__(self) -> None:
        self._counts:     Dict[str, int]   = defaultdict(int)
        self._prev:       Dict[str, int]   = defaultdict(int)
        self._ingested    = 0
        self._lock        = threading.Lock()
        self._cooldown:   Dict[str, float] = {}
        self._pub                          = None
        self._init_redis()

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest(self, text: str, source: str = "unknown") -> List[str]:
        """
        Analyse one text sample. Returns list of matched narrative keys.
        Thread-safe.
        """
        if not text:
            return []

        normalised = text.lower()
        matched    = []

        with self._lock:
            for narrative, keywords in NARRATIVE_KEYWORDS.items():
                hits = [kw for kw in keywords if kw in normalised]
                if hits:
                    self._counts[narrative] += 1
                    matched.append(narrative)

            self._ingested += 1
            if self._ingested % SNAPSHOT_INTERVAL == 0:
                self._check_velocity()

        return matched

    def get_narrative_scores(self) -> Dict[str, float]:
        """
        Returns proportion of ingested texts that matched each narrative.
        Range 0.0–1.0. Used by Phase 6 ensemble as a context weight.
        """
        with self._lock:
            total = max(self._ingested, 1)
            return {
                narrative: round(count / total, 4)
                for narrative, count in self._counts.items()
            }

    def get_dominant_narrative(self) -> Optional[str]:
        """Return the narrative with the highest current count."""
        with self._lock:
            if not self._counts:
                return None
            return max(self._counts, key=self._counts.__getitem__)

    def get_counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
        except Exception as e:
            logger.debug(f"[TopicCluster] Redis unavailable: {e}")

    def _check_velocity(self) -> None:
        """
        Compare current counts to previous snapshot.
        Publish NARRATIVE_TREND_DETECTED on acceleration.
        Called under self._lock.
        """
        for narrative, count in self._counts.items():
            prev = self._prev.get(narrative, 0)
            if count < MIN_COUNT_TO_ALERT:
                continue

            # Velocity = fractional increase since last snapshot
            velocity = (count - prev) / max(prev, 1)

            if velocity < VELOCITY_THRESHOLD:
                continue

            # Rate limit
            last = self._cooldown.get(narrative, 0)
            if time.time() - last < ALERT_COOLDOWN_SECS:
                continue
            self._cooldown[narrative] = time.time()

            strength = (
                "STRONG"   if velocity >= 1.0 else
                "MODERATE" if velocity >= 0.6 else
                "MILD"
            )
            event = {
                "type":             "NARRATIVE_TREND_DETECTED",
                "narrative":        narrative,
                "count":            count,
                "prev":             prev,
                "velocity":         round(velocity, 3),
                "strength":         strength,
                "keywords_matched": NARRATIVE_KEYWORDS[narrative][:4],
                "ts":               int(time.time() * 1000),
            }

            if self._pub:
                try:
                    self._pub.publish(
                        "NARRATIVE_TREND_DETECTED", json.dumps(event)
                    )
                except Exception as e:
                    logger.debug(f"[TopicCluster] Redis publish: {e}")
                    self._pub = None

            logger.info(
                f"[TopicCluster] Trend [{strength}]: {narrative} "
                f"velocity={velocity:.2f} count={count}"
            )

        # Snapshot current counts for next comparison
        self._prev = dict(self._counts)