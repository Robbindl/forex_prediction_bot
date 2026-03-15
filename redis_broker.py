import json
import threading
import os
from datetime import datetime
from typing import Any, Callable, Dict, Optional
from utils.logger import logger


class RedisBroker:
    """
    Thread-safe Redis pub/sub broker with graceful fallback.
    If Redis is unavailable, all publish/subscribe calls are no-ops.
    """

    CHANNELS = [
        'signals',      # trading signals that passed the 7-layer pipeline
        'prices',       # live price ticks per asset
        'whale_alerts', # whale movement events from WhaleAlertManager
        'sentiment',    # composite sentiment updates
        'predictions',  # ML prediction outcomes
        'positions',    # open position updates
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
        if not self._enabled:
            return False
        if not self._ensure_connected():
            return False
        try:
            if isinstance(data, dict):
                data.setdefault('_ts', datetime.utcnow().isoformat())
                data.setdefault('_channel', channel)
            payload = json.dumps(data, default=str)
            with self._lock:
                self._redis.publish(channel, payload)
            return True
        except Exception as e:
            logger.debug(f"[RedisBroker] publish({channel}) failed: {e}")
            self._enabled = False
            return False

    # ── Cache (key-value store for shared state) ───────────────────────────

    def set(self, key: str, value: Any, ttl_seconds: int = 300) -> bool:
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
        if not self._ensure_connected():
            return
        try:
            self._redis.delete(key)
        except Exception:
            pass

    # ── Subscribe (background thread with reconnect) ───────────────────────

    def subscribe(self, channel: str, callback: Callable[[Dict], None]):
        """
        Subscribe to a channel in a background daemon thread.
        callback(data_dict) is called for every message received.
        Includes automatic reconnect on Redis drop.
        """
        if not self._enabled:
            logger.debug(f"[RedisBroker] subscribe({channel}) skipped — Redis unavailable")
            return

        def _listen():
            import time as _time
            while True:
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
                    logger.warning(
                        f"[RedisBroker] subscribe({channel}) connection lost: {e} "
                        f"— retrying in 10s"
                    )
                    _time.sleep(10)

        t = threading.Thread(target=_listen, name=f"RedisSub-{channel}", daemon=True)
        t.start()

    # ── Convenience publishers ──────────────────────────────────────────────

    def publish_signal(self, signal: Dict):
        self.publish('signals', signal)

    def publish_price(self, asset: str, price: float, category: str = ''):
        self.publish('prices', {'asset': asset, 'price': price, 'category': category})

    def publish_whale(self, alert: Dict):
        self.publish('whale_alerts', alert)

    def publish_sentiment(self, asset: str, score: float, label: str):
        self.publish('sentiment', {'asset': asset, 'score': score, 'label': label})

    def publish_prediction(self, asset: str, direction: str,
                           target: float, confidence: float,
                           horizon_minutes: int = 60):
        self.publish('predictions', {
            'asset':           asset,
            'direction':       direction,
            'target_price':    target,
            'confidence':      confidence,
            'horizon_minutes': horizon_minutes,
        })

    def publish_positions(self, positions: list, balance: float):
        self.publish('positions', {'positions': positions, 'balance': balance})

    # ── Status ─────────────────────────────────────────────────────────────

    def is_connected(self) -> bool:
        """Live check — ping Redis so dashboard reflects actual state."""
        if self._redis is None:
            self._connect()
        try:
            if self._redis:
                self._redis.ping()
                self._enabled = True
                return True
        except Exception:
            self._enabled = False
        return False

    def status(self) -> Dict:
        return {'connected': self.is_connected(), 'channels': self.CHANNELS}


# ── Global singleton ──────────────────────────────────────────────────────────
broker = RedisBroker()