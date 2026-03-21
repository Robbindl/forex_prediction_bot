from __future__ import annotations

import json
import os
import threading
import time
from typing import TYPE_CHECKING, Dict, List, Optional

import requests

from utils.logger import get_logger

if TYPE_CHECKING:
    from whale_intelligence.wallet_behavior_classifier import WalletBehaviorClassifier
    from whale_intelligence.wallet_cluster_analyzer   import WalletClusterAnalyzer
    from whale_intelligence.wallet_database           import WalletDatabase

logger = get_logger()

# ── Configuration ─────────────────────────────────────────────────────────────
MIN_BTC_DELTA  = 10.0       # minimum BTC change worth tracking
MIN_ETH_DELTA  = 100.0      # minimum ETH change worth tracking
POLL_INTERVAL  = 300        # seconds between balance checks (5 min)

BLOCKCHAIR_BTC = "https://api.blockchair.com/bitcoin/dashboards/address/{addr}"
ETHERSCAN_BAL  = "https://api.etherscan.io/api"

# ── Seed wallet list ──────────────────────────────────────────────────────────
# Add your own wallets here or use WalletTracker.add_wallet() at runtime.
SEED_WALLETS: List[Dict] = [
    # BTC exchange hot wallets
    {
        "address":  "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
        "label":    "Binance BTC Hot Wallet",
        "chain":    "btc",
        "type":     "exchange",
    },
    {
        "address":  "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97",
        "label":    "Unknown BTC Whale A",
        "chain":    "btc",
        "type":     "unknown",
    },
    # ETH exchange hot wallets
    {
        "address":  "0x28c6c06298d514db089934071355e5743bf21d60",
        "label":    "Binance ETH Hot Wallet",
        "chain":    "eth",
        "type":     "exchange",
    },
    {
        "address":  "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
        "label":    "Binance ETH Cold Wallet",
        "chain":    "eth",
        "type":     "exchange",
    },
]


class WalletTracker:
    """
    Polls on-chain APIs for balance changes on tracked wallets.
    Classifies each movement and publishes Redis events.
    """

    def __init__(
        self,
        db:         "WalletDatabase",
        classifier: "WalletBehaviorClassifier",
        cluster:    "WalletClusterAnalyzer",
        poll_interval_secs: int = POLL_INTERVAL,
    ) -> None:
        self._db         = db
        self._classifier = classifier
        self._cluster    = cluster
        self._interval   = poll_interval_secs
        self._balances:  Dict[str, float]  = {}
        self._wallets:   List[Dict]        = list(SEED_WALLETS)
        self._running    = False
        self._thread:    Optional[threading.Thread] = None
        self._pub                                   = None
        self._eth_key    = os.getenv("ETHERSCAN_API_KEY", "")

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._running = True
        self._init_redis()
        self._thread = threading.Thread(
            target=self._loop, name="WalletTracker", daemon=True
        )
        self._thread.start()
        logger.info(f"[WalletTracker] Tracking {len(self._wallets)} wallets")

    def stop(self) -> None:
        self._running = False

    def add_wallet(self, address: str, label: str,
                   chain: str = "btc", wallet_type: str = "unknown") -> None:
        """Add a wallet at runtime (e.g. from Telegram command)."""
        entry = {"address": address, "label": label,
                 "chain": chain, "type": wallet_type}
        self._wallets.append(entry)
        self._db.upsert_wallet(entry)
        logger.info(f"[WalletTracker] Added wallet: {label} ({address[:12]}...)")

    def get_watched_wallets(self) -> List[Dict]:
        return list(self._wallets)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_redis(self) -> None:
        try:
            import redis
            from config.config import REDIS_URL
            from services.redis_pool import get_client as _get_redis_client

            self._pub = _get_redis_client()
            self._pub.ping()
            logger.info("[WalletTracker] Redis publisher connected")
        except Exception as e:
            logger.warning(f"[WalletTracker] Redis unavailable: {e}")

    def _loop(self) -> None:
        # Load any saved wallets from DB on first run
        saved = self._db.load_all_wallets()
        if saved:
            seen = {w["address"] for w in self._wallets}
            for w in saved:
                if w["address"] not in seen:
                    self._wallets.append(w)

        while self._running:
            for wallet in list(self._wallets):
                try:
                    self._check_wallet(wallet)
                except Exception as e:
                    logger.debug(
                        f"[WalletTracker] {wallet.get('label', '?')} error: {e}"
                    )
                time.sleep(1)   # gentle rate-limiting between API calls
            time.sleep(self._interval)

    def _check_wallet(self, wallet: Dict) -> None:
        chain   = wallet.get("chain", "btc")
        address = wallet["address"]

        balance = (
            self._fetch_btc_balance(address)
            if chain == "btc"
            else self._fetch_eth_balance(address)
        )
        if balance is None:
            return

        prev = self._balances.get(address)
        if prev is None:
            self._balances[address] = balance
            self._db.update_balance(address, balance)
            return

        asset = "BTC" if chain == "btc" else "ETH"
        min_delta = MIN_BTC_DELTA if chain == "btc" else MIN_ETH_DELTA
        delta = balance - prev

        if abs(delta) < min_delta:
            self._balances[address] = balance
            return

        # Classify and publish
        self._balances[address] = balance
        self._db.update_balance(address, balance)
        self._publish_movement(wallet, delta, asset, balance)

    # ── Blockchain API fetchers ───────────────────────────────────────────────

    def _fetch_btc_balance(self, address: str) -> Optional[float]:
        try:
            resp = requests.get(
                BLOCKCHAIR_BTC.format(addr=address),
                timeout=15,
                headers={"User-Agent": "TradingBot/1.0"},
            )
            if resp.status_code == 200:
                data     = resp.json().get("data", {})
                addr_obj = data.get(address, {}).get("address", {})
                sats     = addr_obj.get("balance", 0)
                return float(sats) / 1e8
            if resp.status_code == 402:
                logger.debug("[WalletTracker] Blockchair rate limit hit")
        except Exception as e:
            logger.debug(f"[WalletTracker] BTC fetch {address[:12]}...: {e}")
        return None

    def _fetch_eth_balance(self, address: str) -> Optional[float]:
        if not self._eth_key:
            return None
        try:
            resp = requests.get(
                ETHERSCAN_BAL,
                params={
                    "module":  "account",
                    "action":  "balance",
                    "address": address,
                    "tag":     "latest",
                    "apikey":  self._eth_key,
                },
                timeout=15,
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("status") == "1":
                wei = int(result.get("result", 0))
                return wei / 1e18   # wei → ETH
        except Exception as e:
            logger.debug(f"[WalletTracker] ETH fetch {address[:12]}...: {e}")
        return None

    # ── Event publishing ─────────────────────────────────────────────────────

    def _publish_movement(
        self, wallet: Dict, delta: float, asset: str, new_balance: float
    ) -> None:
        wallet_type = wallet.get("type", "unknown")
        label       = wallet.get("label", "Unknown Whale")
        address     = wallet["address"]
        is_buy      = delta > 0

        # Determine event type
        if wallet_type == "exchange":
            event_type = "EXCHANGE_INFLOW_ALERT" if is_buy else "EXCHANGE_OUTFLOW_ALERT"
        else:
            event_type = "WHALE_ACCUMULATION" if is_buy else "WHALE_DISTRIBUTION"

        event = {
            "type":        event_type,
            "address":     address[:16] + "...",
            "full_address": address,
            "label":       label,
            "wallet_type": wallet_type,
            "asset":       asset,
            "delta":       round(delta, 4),
            "new_balance": round(new_balance, 4),
            "usd_est":     0,   # filled by meta-model when price is available
            "ts":          int(time.time() * 1000),
        }

        # Update classifier profile
        profile = self._db.get_profile(address)
        if profile:
            profile.history.append({"delta": delta, "ts": event["ts"]})
            profile.last_active_ts = event["ts"]
            updated = self._classifier.classify(profile)
            self._db.update_profile(updated)
            event["behavior"] = updated.behavior
            event["behavior_confidence"] = round(updated.confidence, 3)

        # Feed cluster analyser
        self._cluster.ingest(event)

        # Publish to Redis
        if self._pub:
            try:
                self._pub.publish(event_type, json.dumps(event))
            except Exception as e:
                logger.debug(f"[WalletTracker] Redis publish: {e}")

        level = "warning" if "EXCHANGE" in event_type else "info"
        getattr(logger, level)(
            f"[WalletTracker] {label}: {delta:+.4f} {asset} → [{event_type}]"
        )