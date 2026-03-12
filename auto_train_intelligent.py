"""
Intelligent Auto-Training System with Event Detection
Trains ML models based on:
- Significant price movements (>2%)
- New trading sessions (London, NY, Asia open)
- Major news/economic events (using your APIs)
- Time since last training (4 hour fallback)
"""

import threading
import time
from datetime import datetime, timedelta
import pickle
import os
import json
import requests
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from data.fetcher import MarketHours
from logger import logger

class IntelligentAutoTrainer:
    """
    Automatically retrains ML models based on market events:
    - Price movements (>2% triggers training)
    - Session changes (London, NY, Asia opens)
    - News events (via multiple APIs)
    - Time fallback (every 4 hours)
    """
    
    def __init__(self, trading_system):
        self.bot = trading_system
        self.last_trained: Dict[str, datetime] = {}
        self.last_session_check = None
        self.current_session = None
        self.training_history: List[Dict] = []
        self.is_running = False
        self.trainer_thread = None
        
        # Price tracking for movement detection
        self.price_history: Dict[str, List[Dict]] = {}
        
        # ===== YOUR NEWS API KEYS =====
        self.newsapi_key = os.getenv("NEWSAPI_KEY", "")
        self.gnews_key = os.getenv("GNEWS_KEY", "")
        self.rapidapi_key = os.getenv("RAPIDAPI_KEY", "")
        
        # Create models directory
        os.makedirs("ml_models", exist_ok=True)
        
        logger.info("🤖 Intelligent Auto-Trainer initialized")

        logger.info("   • Price movement threshold: 2%")

        logger.info("   • Session detection: London, NY, Asia")

        logger.info("   • News sources: NewsAPI, GNews, RapidAPI")

        logger.info("   • Time fallback: Every 4 hours")

    def detect_price_movement(self, asset: str, current_price: float) -> Tuple[bool, float]:
        """
        Detect significant price movements
        Returns: (is_significant, movement_pct)
        """
        if asset not in self.price_history:
            self.price_history[asset] = []
        
        # Add current price with timestamp
        self.price_history[asset].append({
            'price': current_price,
            'time': datetime.now()
        })
        
        # Keep last hour of prices
        cutoff = datetime.now() - timedelta(hours=1)
        self.price_history[asset] = [
            p for p in self.price_history[asset] 
            if p['time'] > cutoff
        ]
        
        if len(self.price_history[asset]) < 2:
            return False, 0
        
        # Calculate movement from oldest in last hour
        oldest = self.price_history[asset][0]['price']
        movement_pct = abs((current_price - oldest) / oldest) * 100
        
        return movement_pct > 2, movement_pct
    
    def detect_session_change(self) -> Tuple[bool, str]:
        """
        Detect new trading sessions
        Sessions: Asia (Tokyo), London, New York
        """
        now = datetime.now()
        
        # Convert to NY time for session detection
        ny_time = MarketHours.get_ny_time()
        hour = ny_time.hour
        
        sessions = {
            (0, 8): "Asia/Tokyo",
            (8, 16): "London",
            (16, 24): "New York"
        }
        
        new_session = None
        for (start, end), session_name in sessions.items():
            if start <= hour < end:
                new_session = session_name
                break
        
        # Check if session changed
        session_changed = (self.current_session != new_session)
        self.current_session = new_session
        
        return session_changed, new_session if new_session else "Unknown"
    
    def detect_news_newsapi(self, asset: str) -> Tuple[bool, str]:
        """Detect news using NewsAPI"""
        try:
            # Map asset to search term
            search_terms = {
                'BTC-USD': 'bitcoin',
                'ETH-USD': 'ethereum',
                'BNB-USD': 'binance coin',
                'SOL-USD': 'solana',
                'XRP-USD': 'xrp ripple',
                'AAPL': 'Apple',
                'MSFT': 'Microsoft',
                'GOOGL': 'Google',
                'AMZN': 'Amazon',
                'TSLA': 'Tesla',
                'NVDA': 'NVIDIA',
                'EUR/USD': 'euro dollar',
                'GBP/USD': 'pound sterling',
                'USD/JPY': 'dollar yen',
                'GC=F': 'gold',
                'SI=F': 'silver',
                'CL=F': 'crude oil',
            }
            
            query = search_terms.get(asset, asset.split('-')[0])
            
            # Check for news in last hour
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'from': (datetime.now() - timedelta(hours=1)).isoformat(),
                'sortBy': 'relevancy',
                'language': 'en',
                'pageSize': 5,
                'apiKey': self.newsapi_key
            }
            
            response = requests.get(url, params=params, timeout=3)
            data = response.json()
            
            if data.get('status') == 'ok' and data.get('totalResults', 0) > 0:
                # Check if any major news (headline contains key words)
                for article in data['articles'][:5]:
                    title = article.get('title', '').lower()
                    description = article.get('description', '').lower()
                    text = title + ' ' + description
                    
                    keywords = ['surge', 'plunge', 'crisis', 'boom', 'crash', 
                               'soar', 'tumble', 'rally', 'dump', 'pump',
                               'earnings', 'fed', 'interest rate', 'inflation']
                    
                    if any(word in text for word in keywords):
                        return True, f"NewsAPI: {article['title'][:50]}"
            
        except Exception as e:
            pass
        
        return False, ""
    
    def detect_news_gnews(self, asset: str) -> Tuple[bool, str]:
        """Detect news using GNews API"""
        try:
            search_terms = {
                'BTC-USD': 'bitcoin',
                'ETH-USD': 'ethereum',
                'AAPL': 'Apple',
                'EUR/USD': 'euro dollar',
                'GC=F': 'gold',
            }
            
            query = search_terms.get(asset, asset.split('-')[0])
            
            url = "https://gnews.io/api/v4/search"
            params = {
                'q': query,
                'lang': 'en',
                'max': 5,
                'apikey': self.gnews_key
            }
            
            response = requests.get(url, params=params, timeout=3)
            data = response.json()
            
            if data.get('articles'):
                for article in data['articles'][:5]:
                    title = article.get('title', '').lower()
                    description = article.get('description', '').lower()
                    text = title + ' ' + description
                    
                    keywords = ['surge', 'plunge', 'crisis', 'boom', 'crash']
                    
                    if any(word in text for word in keywords):
                        return True, f"GNews: {article['title'][:50]}"
            
        except Exception as e:
            pass
        
        return False, ""
    
    def detect_major_news(self, asset: str) -> Tuple[bool, str]:
        """
        Detect major news events for an asset using multiple APIs
        """
        # Try all news sources
        news_sources = [
            self.detect_news_newsapi,
            self.detect_news_gnews,
        ]
        
        for source_func in news_sources:
            found, title = source_func(asset)
            if found:
                return True, title
        
        return False, ""
    
    def should_train_asset(self, asset: str, category: str, current_price: float = None) -> Tuple[bool, str]:
        """
        Determine if an asset should be retrained based on multiple factors
        
        Returns:
            (should_train: bool, reason: str)
        """
        now = datetime.now()
        
        # Skip if market closed
        if not MarketHours.get_status().get(category, False):
            return False, "market closed"
        
        # Get last training time
        last_time = self.last_trained.get(asset)
        
        # ===== FACTOR 1: Significant price movement =====
        if current_price:
            is_significant, movement = self.detect_price_movement(asset, current_price)
            if is_significant:
                return True, f"price moved {movement:.1f}%"
        
        # ===== FACTOR 2: New trading session =====
        session_changed, session_name = self.detect_session_change()
        if session_changed and last_time:
            hours_since = (now - last_time).total_seconds() / 3600
            if hours_since > 1:  # Don't retrain immediately if just trained
                return True, f"new session: {session_name}"
        
        # ===== FACTOR 3: Major news =====
        has_news, news_title = self.detect_major_news(asset)
        if has_news and last_time:
            hours_since = (now - last_time).total_seconds() / 3600
            if hours_since > 1:
                return True, f"news: {news_title[:40]}"
        
        # ===== FACTOR 4: Time-based fallback =====
        if not last_time:
            return True, "first training"
        
        hours_since = (now - last_time).total_seconds() / 3600
        
        if hours_since >= 24:
            return True, f"{hours_since:.1f}h old (max)"
        
        if hours_since >= 4:
            return True, f"{hours_since:.1f}h old"
        
        return False, "too recent"
    
    def train_asset(self, asset: str, category: str, reason: str, current_price: float = None) -> bool:
        """
        Train a single asset
        """
        try:
            logger.info(f"\n🔄 Training {asset} - {reason}")

            # Fetch fresh data (more data for training)
            df = self.bot.fetch_historical_data(asset, days=60, interval='15m')
            if df.empty or len(df) < 100:
                logger.info(f"   ⚠️ Insufficient data for {asset}")

                return False
            
            # Add indicators
            from indicators.technical import TechnicalIndicators
            df = TechnicalIndicators.add_all_indicators(df)
            
            # Train model
            self.bot.predictor.train(df, target_periods=5)
            
            # Save model
            safe_asset = asset.replace('/', '_').replace('\\', '_').replace(':', '_')
            model_path = f"ml_models/{safe_asset}_model.pkl"
            
            with open(model_path, 'wb') as f:
                pickle.dump(self.bot.predictor, f)
            
            # Update tracking
            self.last_trained[asset] = datetime.now()
            self.training_history.append({
                'asset': asset,
                'timestamp': datetime.now(),
                'reason': reason,
                'data_points': len(df),
                'success': True
            })
            
            logger.info(f"   ✅ {asset} trained successfully ({len(df)} data points)")

            return True
            
        except Exception as e:
            logger.info(f"   ❌ {asset} training failed: {e}")

            self.training_history.append({
                'asset': asset,
                'timestamp': datetime.now(),
                'reason': reason,
                'success': False,
                'error': str(e)
            })
            return False
    
    def training_loop(self):
        """Main training loop - runs in background thread"""
        logger.info("\n🚀 Intelligent Auto-Training started")

        logger.info("   Monitoring: price movements, session changes, news events")

        while self.is_running:
            try:
                # Get all assets from trading system
                assets = self.bot.get_asset_list()
                
                # First, get current prices for movement detection
                current_prices = {}
                for asset, category in assets:
                    if MarketHours.get_status().get(category, False):
                        price, _ = self.bot.fetcher.get_real_time_price(asset, category)
                        if price:
                            current_prices[asset] = price
                
                # Check which assets need training
                to_train = []
                for asset, category in assets:
                    price = current_prices.get(asset)
                    should_train, reason = self.should_train_asset(asset, category, price)
                    if should_train:
                        to_train.append((asset, category, reason, price))
                
                # Train assets that need it
                if to_train:
                    logger.info(f"\n📊 Training queue: {len(to_train)} assets need updates")

                    # Sort by priority: price movement > news > session > time
                    for asset, category, reason, price in to_train[:3]:  # Train 3 at a time
                        self.train_asset(asset, category, reason, price)
                        time.sleep(5)  # Delay between training to avoid rate limits
                
                # Wait before next check
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.info(f"⚠️ Training loop error: {e}")

                time.sleep(60)
    
    def start(self):
        """Start the auto-trainer in background thread"""
        if self.is_running:
            logger.info("⚠️ Auto-trainer already running")

            return
        
        self.is_running = True
        self.trainer_thread = threading.Thread(target=self.training_loop, daemon=True)
        self.trainer_thread.start()
        logger.info("✅ Intelligent Auto-Trainer started")

    def stop(self):
        """Stop the auto-trainer"""
        self.is_running = False
        if self.trainer_thread:
            self.trainer_thread.join(timeout=5)
        logger.info("🛑 Auto-Trainer stopped")

    def get_status(self) -> Dict:
        """Get training status"""
        now = datetime.now()
        status = {
            'total_trained': len(self.last_trained),
            'current_session': self.current_session,
            'assets': {}
        }
        
        for asset, last_time in self.last_trained.items():
            hours_ago = (now - last_time).total_seconds() / 3600
            status['assets'][asset] = {
                'last_trained': last_time.isoformat(),
                'hours_ago': round(hours_ago, 1)
            }
        
        # Recent training history
        status['recent'] = self.training_history[-5:] if self.training_history else []
        
        return status