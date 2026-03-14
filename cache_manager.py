"""
Cache Manager - Redis cache for real-time data to reduce API calls
"""

import redis
import json
import pickle
from typing import Optional, Any, Dict
from datetime import timedelta
from utils.logger import logger

class CacheManager:
    """
    Redis cache for real-time data to reduce API calls
    """
    
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.redis_client = None
        self.enabled = False
        
        try:
            if password:
                self.redis_client = redis.Redis(
                    host=host, port=port, db=db, 
                    password=password, decode_responses=True
                )
            else:
                self.redis_client = redis.Redis(
                    host=host, port=port, db=db, decode_responses=True
                )
            
            # Test connection
            self.redis_client.ping()
            self.enabled = True
            logger.info(f"✅ Redis cache connected at {host}:{port}")

        except Exception as e:
            logger.info(f"⚠️ Redis not available (using in-memory cache): {e}")

            self.enabled = False
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self.enabled:
            return None
        
        try:
            value = self.redis_client.get(key)
            if value:
                return pickle.loads(value)
            return None
        except:
            return None
    
    def set(self, key: str, value: Any, ttl_seconds: int = 30):
        """Set value in cache with TTL"""
        if not self.enabled:
            return
        
        try:
            pickled = pickle.dumps(value)
            self.redis_client.setex(key, ttl_seconds, pickled)
        except:
            pass
    
    def get_price(self, asset: str, source: str) -> Optional[float]:
        """Get cached price"""
        key = f"price:{asset}:{source}"
        return self.get(key)
    
    def set_price(self, asset: str, source: str, price: float, ttl: int = 30):
        """Cache price"""
        key = f"price:{asset}:{source}"
        self.set(key, price, ttl)
    
    def get_signal(self, asset: str) -> Optional[Dict]:
        """Get cached signal"""
        key = f"signal:{asset}"
        return self.get(key)
    
    def set_signal(self, asset: str, signal: Dict, ttl: int = 60):
        """Cache signal"""
        key = f"signal:{asset}"
        self.set(key, signal, ttl)
    
    def get_historical_data(self, asset: str, interval: str) -> Optional[Any]:
        """Get cached historical data"""
        key = f"historical:{asset}:{interval}"
        return self.get(key)
    
    def set_historical_data(self, asset: str, interval: str, data: Any, ttl: int = 300):
        """Cache historical data (5 min TTL)"""
        key = f"historical:{asset}:{interval}"
        self.set(key, data, ttl)
    
    def get_correlation(self, asset1: str, asset2: str) -> Optional[float]:
        """Get cached correlation"""
        key = f"corr:{asset1}:{asset2}"
        return self.get(key)
    
    def set_correlation(self, asset1: str, asset2: str, corr: float, ttl: int = 3600):
        """Cache correlation (1 hour TTL)"""
        key = f"corr:{asset1}:{asset2}"
        self.set(key, corr, ttl)
    
    def clear(self):
        """Clear all cache"""
        if self.enabled:
            self.redis_client.flushdb()
            logger.info("✅ Cache cleared")