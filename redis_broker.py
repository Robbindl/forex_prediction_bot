"""
redis_broker.py — Redis pub/sub layer for the trading platform
==============================================================
All Python services use this module to publish events to Redis.
The Node.js gateway subscribes and broadcasts to WebSocket clients.

Channels:
  signals       — trading signals that passed the 7-layer gate
  prices        — live price ticks {asset, price, timestamp}
  whale_alerts  — whale movement events
  sentiment     — sentiment score updates
  orderflow     — bid/ask delta and imbalance data
  alpha         — alpha discovery engine signals
  predictions   — AI price predictions
  positions     — open position updates

Usage:
  from redis_broker import broker
  broker.publish('signals', signal_dict)
  broker.publish('prices', {'asset': 'EUR/USD', 'price': 1.0841})

If Redis is not running, every call is a no-op — the bot keeps working.
"""

import json
import threading
import os
from datetime import datetime
from typing import Any, Callable, Dict, Optional
from logger import logger


class RedisBroker:
    """
    Thread-safe Redis pub/sub broker with graceful fallback.
    If Redis is unavailable, all publish/subscribe calls are no-ops.
    """

    CHANNELS = [
        'signals', 'prices', 'whale_alerts', 'sentiment',
        'orderflow', 'alpha', 'predictions', 'positions',
    ]

    def __init__(self):
        self._redis   = None
        self._lock    = threading.Lock()
        self._enabled = False
        self._connect()

    # ── Connection ─────────────────────────────────────────────────────────

    def _connect(self):
        """Try to connect to Redis. Silently fails if unavailable."""
        try:
            import redis
            host     = os.getenv('REDIS_HOST', '127.0.0.1')
            port     = int(os.getenv('REDIS_PORT', '6379'))
            password = os.getenv('REDIS_PASSWORD') or None
            db       = int(os.getenv('REDIS_DB', '0'))

            self._redis = redis.Redis(
                host=host, port=port, password=password, db=db,
                socket_connect_timeout=2,
                socket_timeout=2,
                decode_responses=True,
                retry_on_timeout=True,
            )
            # Test connection
            self._redis.ping()
            self._enabled = True
            logger.info(f"[RedisBroker] Connected to {host}:{port}")
        except ImportError:
            logger.warning("[RedisBroker] redis-py not installed — install with: pip install redis")
            self._enabled = False
        except Exception as e:
            logger.warning(f"[RedisBroker] Redis unavailable ({e}) — running without Redis (no WebSocket broadcast)")
            self._enabled = False

    def _ensure_connected(self) -> bool:
        """Reconnect if connection dropped."""
        if not self._enabled:
            return False
        try:
            self._redis.ping()
            return True
        except Exception:
            try:
                self._connect()
                return self._enabled
            except Exception:
                return False

    # ── Publish ────────────────────────────────────────────────────────────

    def publish(self, channel: str, data: Any) -> bool:
        """
        Publish data to a Redis channel.
        data can be a dict, list, str, or number — it will be JSON-serialised.
        Returns True if published, False if Redis unavailable.
        """
        if not self._enabled:
            return False
        if not self._ensure_connected():
            return False

        try:
            # Add standard metadata
            if isinstance(data, dict):
                data.setdefault('_ts', datetime.utcnow().isoformat())
                data.setdefault('_channel', channel)

            payload = json.dumps(data, default=str)
            with self._lock:
                self._redis.publish(channel, payload)
            return True
        except Exception as e:
            logger.debug(f"[RedisBroker] publish({channel}) failed: {e}")
            self._enabled = False   # Mark as disconnected; retry on next call
            return False

    # ── Cache (key-value store for shared state) ───────────────────────────

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> bool:
        """Store a value in Redis with TTL. Used for shared state between services."""
        if not self._ensure_connected():
            return False
        try:
            payload = json.dumps(value, default=str)
            with self._lock:
                self._redis.setex(key, ttl_seconds, payload)
            return True
        except Exception as e:
            logger.debug(f"[RedisBroker] set({key}) failed: {e}")
            return False

    def get(self, key: str, default=None) -> Any:
        """Retrieve a value from Redis."""
        if not self._ensure_connected():
            return default
        try:
            raw = self._redis.get(key)
            if raw is None:
                return default
            return json.loads(raw)
        except Exception:
            return default

    def delete(self, key: str):
        """Delete a key from Redis."""
        if not self._ensure_connected():
            return
        try:
            self._redis.delete(key)
        except Exception:
            pass

    # ── Subscribe (background thread) ─────────────────────────────────────

    def subscribe(self, channel: str, callback: Callable[[Dict], None]):
        """
        Subscribe to a channel in a background daemon thread.
        callback(data_dict) is called for every message received.
        """
        if not self._enabled:
            logger.debug(f"[RedisBroker] subscribe({channel}) skipped — Redis unavailable")
            return

        def _listen():
            try:
                import redis as _r
                host     = os.getenv('REDIS_HOST', '127.0.0.1')
                port     = int(os.getenv('REDIS_PORT', '6379'))
                password = os.getenv('REDIS_PASSWORD') or None

                sub_client = _r.Redis(
                    host=host, port=port, password=password,
                    socket_connect_timeout=5,
                    decode_responses=True,
                )
                ps = sub_client.pubsub()
                ps.subscribe(channel)
                logger.info(f"[RedisBroker] Listening on channel '{channel}'")

                for msg in ps.listen():
                    if msg['type'] == 'message':
                        try:
                            data = json.loads(msg['data'])
                            callback(data)
                        except Exception as e:
                            logger.debug(f"[RedisBroker] callback error on {channel}: {e}")
            except Exception as e:
                logger.warning(f"[RedisBroker] subscribe({channel}) thread died: {e}")

        t = threading.Thread(target=_listen, name=f"RedisSub-{channel}", daemon=True)
        t.start()

    # ── Convenience publishers ──────────────────────────────────────────────

    def publish_signal(self, signal: Dict):
        """Publish a trading signal that passed the quality gate."""
        self.publish('signals', signal)

    def publish_price(self, asset: str, price: float, category: str = ''):
        """Publish a live price tick."""
        self.publish('prices', {
            'asset':    asset,
            'price':    price,
            'category': category,
        })

    def publish_whale(self, alert: Dict):
        """Publish a whale movement alert."""
        self.publish('whale_alerts', alert)

    def publish_sentiment(self, asset: str, score: float, label: str):
        """Publish a sentiment update."""
        self.publish('sentiment', {
            'asset': asset,
            'score': score,
            'label': label,
        })

    def publish_orderflow(self, data: Dict):
        """Publish order flow data (bid/ask imbalance)."""
        self.publish('orderflow', data)

    def publish_alpha(self, signal: Dict):
        """Publish an alpha discovery signal."""
        self.publish('alpha', signal)

    def publish_prediction(self, asset: str, direction: str,
                           target: float, confidence: float,
                           horizon_minutes: int = 60):
        """Publish an AI price prediction."""
        self.publish('predictions', {
            'asset':           asset,
            'direction':       direction,
            'target_price':    target,
            'confidence':      confidence,
            'horizon_minutes': horizon_minutes,
        })

    def publish_positions(self, positions: list, balance: float):
        """Publish current open positions update."""
        self.publish('positions', {
            'positions': positions,
            'balance':   balance,
        })

    # ── Status ─────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._enabled

    def status(self) -> Dict:
        return {
            'connected': self._enabled,
            'channels':  self.CHANNELS,
        }


# ── Global singleton ──────────────────────────────────────────────────────────
broker = RedisBroker()
