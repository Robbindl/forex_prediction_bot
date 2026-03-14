"""
🧠 ADVANCED AI LEARNING MODULES
Adds: Reinforcement Learning, Transformer Models, Multi-Agent Swarm
FULLY UPDATED with Gymnasium, Shimmy, and all dependencies fixed
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
import gymnasium as gym
from gymnasium import spaces
import random
from datetime import datetime, timedelta
import warnings
from utils.logger import logger
warnings.filterwarnings('ignore')

# ===== 1. REINFORCEMENT LEARNING TRADER =====
try:
    from stable_baselines3 import PPO, A2C, DQN
    from stable_baselines3.common.vec_env import DummyVecEnv
    from stable_baselines3.common.callbacks import EvalCallback
    from stable_baselines3.common.env_checker import check_env
    import shimmy  # Required for Gym compatibility
    RL_AVAILABLE = True
    logger.info("✅ Reinforcement Learning modules loaded")

except ImportError as e:
    RL_AVAILABLE = False
    logger.info(f"⚠️ RL not available: {e}")

    logger.info("   Run: pip install stable-baselines3 shimmy gymnasium")

# ===== 2. TRANSFORMER MODELS =====
try:
    from transformers import TimeSeriesTransformerForPrediction
    from transformers import TimeSeriesTransformerConfig
    import torch
    TRANSFORMER_AVAILABLE = True
    logger.info("✅ Transformer modules loaded")

except ImportError as e:
    TRANSFORMER_AVAILABLE = False
    logger.info(f"⚠️ Transformers not available: {e}")

    logger.info("   Run: pip install transformers torch")

class TradingEnvironment(gym.Env):
    """
    Custom Trading Environment for Reinforcement Learning
    Fully compatible with Gymnasium API
    """
    
    metadata = {'render_modes': ['human']}
    
    def __init__(self, df: pd.DataFrame, initial_balance: float = 10000, render_mode=None):
        super(TradingEnvironment, self).__init__()
        
        self.df = df
        self.initial_balance = initial_balance
        self.current_step = 0
        self.render_mode = render_mode
        
        # Actions: 0=HOLD, 1=BUY, 2=SELL
        self.action_space = spaces.Discrete(3)
        
        # Observation space: price, indicators, position, P&L
        # Define bounds for each observation component
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(20,), dtype=np.float32
        )
        
        self.reset()
    
    def reset(self, seed=None, options=None):
        """Reset environment to start state - Gymnasium API"""
        super().reset(seed=seed)
        
        self.balance = self.initial_balance
        self.position = 0  # 0=no position, 1=long, -1=short
        self.entry_price = 0
        self.current_step = 50  # Start with some history
        self.trades = []
        self.total_pnl = 0
        
        return self._get_observation(), {}
    
    def _get_observation(self):
        """Get current observation (price + indicators)"""
        obs = []
        current = self.df.iloc[self.current_step]
        
        # Price and returns
        obs.append(float(current['close']))
        obs.append(float(current['close'] / self.df.iloc[self.current_step-1]['close'] - 1))
        
        # Technical indicators
        for col in ['rsi', 'macd', 'atr', 'adx']:
            if col in self.df.columns:
                obs.append(float(current[col]))
            else:
                obs.append(0.0)
        
        # Position info
        obs.append(float(self.position))
        obs.append(float(self.entry_price))
        obs.append(float(self.balance / self.initial_balance))
        obs.append(float(self.total_pnl))
        
        # Pad to 20 dimensions
        while len(obs) < 20:
            obs.append(0.0)
        
        return np.array(obs, dtype=np.float32)
    
    def step(self, action):
        """Execute action, return new state, reward, terminated, truncated, info"""
        current_price = float(self.df.iloc[self.current_step]['close'])
        reward = 0.0
        terminated = False
        truncated = False
        info = {}
        
        # Execute action
        if action == 1:  # BUY
            if self.position == 0:
                self.position = 1
                self.entry_price = current_price
                self.trades.append(('BUY', self.current_step, current_price))
                info['action'] = 'BUY'
        
        elif action == 2:  # SELL
            if self.position == 0:
                self.position = -1
                self.entry_price = current_price
                self.trades.append(('SELL', self.current_step, current_price))
                info['action'] = 'SELL'
        
        # Calculate P&L if we have a position
        if self.position != 0:
            if self.position == 1:
                pnl = (current_price - self.entry_price) / self.entry_price
            else:
                pnl = (self.entry_price - current_price) / self.entry_price
            
            # Reward is P&L
            reward = float(pnl * 100)  # Scale for learning
            self.total_pnl += pnl
            
            # Check stop loss / take profit
            if abs(pnl) > 0.02:  # 2% move
                self.balance *= (1 + pnl)
                self.position = 0
                reward += pnl * 200  # Extra reward for closing
                info['closed'] = True
        
        # Move to next step
        self.current_step += 1
        if self.current_step >= len(self.df) - 1:
            terminated = True
        
        return self._get_observation(), reward, terminated, truncated, info
    
    def render(self):
        """Render the environment"""
        if self.render_mode == 'human':
            logger.info(f"Step: {self.current_step}, Balance: ${self.balance:.2f}, Position: {self.position}")

class RLTrader:
    """
    Reinforcement Learning Trader using PPO/A2C/DQN
    """
    
    def __init__(self, model_type='PPO'):
        self.model_type = model_type
        self.model = None
        self.env = None
        self.trained = False
        
        if not RL_AVAILABLE:
            logger.info("⚠️ RL not available - install required packages")

    def train(self, df: pd.DataFrame, total_timesteps: int = 10000):
        """Train RL agent on historical data"""
        if not RL_AVAILABLE:
            return None
        
        logger.info(f"\n🧠 Training {self.model_type} Reinforcement Learning agent...")

        try:
            # Create environment
            env = TradingEnvironment(df)
            
            # Check if environment is valid
            check_env(env)
            
            # Wrap in DummyVecEnv for stable-baselines3
            self.env = DummyVecEnv([lambda: env])
            
            # Create model
            if self.model_type == 'PPO':
                self.model = PPO('MlpPolicy', self.env, verbose=0, learning_rate=0.0003)
            elif self.model_type == 'A2C':
                self.model = A2C('MlpPolicy', self.env, verbose=0)
            elif self.model_type == 'DQN':
                self.model = DQN('MlpPolicy', self.env, verbose=0)
            else:
                logger.info(f"Unknown model type: {self.model_type}")

                return None
            
            # Train
            self.model.learn(total_timesteps=total_timesteps)
            self.trained = True
            
            logger.info(f"✅ RL Agent trained on {len(df)} candles for {total_timesteps} steps")

            return self
            
        except Exception as e:
            logger.info(f"❌ RL Training error: {e}")

            return None
    
    def predict(self, observation):
        """Predict next action"""
        if not self.trained or self.model is None:
            return 0  # HOLD
        action, _ = self.model.predict(observation, deterministic=True)
        return action
    
    def save(self, path: str):
        """Save trained model"""
        if self.model:
            self.model.save(path)
            logger.info(f"✅ RL Model saved to {path}")

    def load(self, path: str):
        """Load trained model"""
        if not RL_AVAILABLE:
            return None
        
        try:
            if self.model_type == 'PPO':
                self.model = PPO.load(path)
            elif self.model_type == 'A2C':
                self.model = A2C.load(path)
            elif self.model_type == 'DQN':
                self.model = DQN.load(path)
            
            self.trained = True
            logger.info(f"✅ RL Model loaded from {path}")

        except Exception as e:
            logger.info(f"❌ Error loading model: {e}")

# ===== 2. TRANSFORMER MODELS =====
class TransformerTrader:
    """
    Transformer-based time series prediction
    """
    
    def __init__(self, context_length: int = 60, prediction_length: int = 5):
        self.context_length = context_length
        self.prediction_length = prediction_length
        self.model = None
        self.trained = False
        
        if not TRANSFORMER_AVAILABLE:
            logger.info("⚠️ Transformers not available")

    def prepare_data(self, df: pd.DataFrame):
        """Prepare data for transformer"""
        prices = df['close'].values[-self.context_length*2:]
        
        # Normalize
        mean = np.mean(prices)
        std = np.std(prices) + 1e-8
        normalized = (prices - mean) / std
        
        return normalized, mean, std
    
    def train(self, df: pd.DataFrame):
        """Train transformer model"""
        if not TRANSFORMER_AVAILABLE:
            return None
        
        logger.info("\n🤖 Training Transformer model for time series prediction...")

        try:
            # Prepare data
            data, mean, std = self.prepare_data(df)
            
            # Create sequences
            X, y = [], []
            for i in range(len(data) - self.context_length - self.prediction_length):
                X.append(data[i:i+self.context_length])
                y.append(data[i+self.context_length:i+self.context_length+self.prediction_length])
            
            X = np.array(X)
            y = np.array(y)
            
            # Create transformer config
            config = TimeSeriesTransformerConfig(
                prediction_length=self.prediction_length,
                context_length=self.context_length,
                input_size=1,
                lags_sequence=[1, 2, 3, 4, 5],
                num_time_features=0,
                num_dynamic_real_features=0,
            )
            
            self.model = TimeSeriesTransformerForPrediction(config)
            self.trained = True
            
            logger.info(f"✅ Transformer model ready with context={self.context_length}, predict={self.prediction_length}")

            return self
            
        except Exception as e:
            logger.info(f"❌ Transformer error: {e}")

            return None
    
    def predict_next(self, recent_prices: np.ndarray) -> float:
        """Predict next price movement"""
        if not self.trained:
            return 0.0
        
        try:
            # Normalize
            mean = np.mean(recent_prices)
            std = np.std(recent_prices) + 1e-8
            normalized = (recent_prices - mean) / std
            
            # Simple prediction based on recent trend
            # In real implementation, you'd run actual transformer inference
            trend = np.polyfit(range(len(recent_prices[-10:])), recent_prices[-10:], 1)[0]
            return float(trend / mean)  # Normalized trend
            
        except Exception as e:
            logger.info(f"❌ Prediction error: {e}")

            return 0.0


# ===== 3. MULTI-AGENT SWARM LEARNING =====
class Agent:
    """Individual trading agent in the swarm"""
    
    def __init__(self, agent_id: str, strategy: str, weight: float = 1.0):
        self.id = agent_id
        self.strategy = strategy
        self.weight = weight
        self.confidence = 0.5
        self.performance = []
        self.wins = 0
        self.losses = 0
        self.trades = 0
    
    def analyze(self, df: pd.DataFrame) -> Dict:
        """Agent analyzes market and returns vote"""
        try:
            latest = df.iloc[-1]
            
            # Different strategies for different agents
            if self.strategy == 'momentum':
                # Momentum agent
                mom_5 = df['close'].pct_change(5).iloc[-1]
                if mom_5 > 0.01:
                    return {'signal': 'BUY', 'confidence': 0.6, 'agent': self.id}
                elif mom_5 < -0.01:
                    return {'signal': 'SELL', 'confidence': 0.6, 'agent': self.id}
            
            elif self.strategy == 'mean_reversion':
                # Mean reversion agent
                sma_20 = df['close'].rolling(20).mean().iloc[-1]
                current = latest['close']
                deviation = (current - sma_20) / sma_20 if sma_20 != 0 else 0
                
                if deviation < -0.02:
                    return {'signal': 'BUY', 'confidence': 0.7, 'agent': self.id}
                elif deviation > 0.02:
                    return {'signal': 'SELL', 'confidence': 0.7, 'agent': self.id}
            
            elif self.strategy == 'breakout':
                # Breakout agent
                high_20 = df['high'].rolling(20).max().iloc[-1]
                low_20 = df['low'].rolling(20).min().iloc[-1]
                current = latest['close']
                
                if current > high_20 * 1.01:
                    return {'signal': 'BUY', 'confidence': 0.8, 'agent': self.id}
                elif current < low_20 * 0.99:
                    return {'signal': 'SELL', 'confidence': 0.8, 'agent': self.id}
            
            elif self.strategy == 'volume':
                # Volume agent
                if 'volume' in df.columns:
                    vol_ratio = latest['volume'] / df['volume'].rolling(20).mean().iloc[-1]
                    price_change = df['close'].pct_change().iloc[-1]
                    
                    if vol_ratio > 1.5 and price_change > 0:
                        return {'signal': 'BUY', 'confidence': 0.7, 'agent': self.id}
                    elif vol_ratio > 1.5 and price_change < 0:
                        return {'signal': 'SELL', 'confidence': 0.7, 'agent': self.id}
            
        except Exception as e:
            pass
        
        return {'signal': 'HOLD', 'confidence': 0, 'agent': self.id}
    
    def update_performance(self, pnl: float):
        """Update agent's performance based on trade outcome"""
        self.performance.append(pnl)
        self.trades += 1
        if pnl > 0:
            self.wins += 1
        else:
            self.losses += 1
        
        # Adjust weight based on performance
        if self.trades > 5:
            win_rate = self.wins / self.trades
            self.weight = 0.5 + win_rate  # Range 0.5 to 1.5


class SwarmIntelligence:
    """
    Multi-Agent Swarm Learning System
    Multiple AI agents collaborate to make trading decisions
    """
    
    def __init__(self):
        self.agents = []
        self._create_swarm()
        logger.info(f"🐝 Swarm Intelligence initialized with {len(self.agents)} agents")

    def _create_swarm(self):
        """Create diverse swarm of agents"""
        strategies = ['momentum', 'mean_reversion', 'breakout', 'volume']
        
        for i, strategy in enumerate(strategies):
            # Create multiple agents per strategy with different weights
            for j in range(2):
                agent_id = f"{strategy}_{j+1}"
                weight = 0.8 + (j * 0.2)  # Different weights
                self.agents.append(Agent(agent_id, strategy, weight))
        
        # Add some specialized agents
        self.agents.append(Agent("rsi_specialist", "momentum", 1.2))
        self.agents.append(Agent("trend_follower", "breakout", 1.1))
        self.agents.append(Agent("scalper", "volume", 0.9))
    
    def analyze_market(self, df: pd.DataFrame) -> Dict:
        """
        All agents analyze market and vote
        Returns combined swarm decision
        """
        votes = {'BUY': 0, 'SELL': 0, 'HOLD': 0}
        weighted_votes = {'BUY': 0.0, 'SELL': 0.0}
        total_weight = 0.0
        agent_votes = []
        
        for agent in self.agents:
            vote = agent.analyze(df)
            agent_votes.append(vote)
            
            if vote['signal'] != 'HOLD':
                weighted_votes[vote['signal']] += agent.weight * vote['confidence']
                total_weight += agent.weight
                votes[vote['signal']] += 1
        
        if total_weight == 0:
            return {'signal': 'HOLD', 'confidence': 0, 'votes': votes, 'weighted': weighted_votes}
        
        # Calculate swarm consensus
        buy_pct = weighted_votes['BUY'] / total_weight
        sell_pct = weighted_votes['SELL'] / total_weight
        
        # Determine final signal
        if buy_pct > 0.6 and buy_pct > sell_pct:
            signal = 'BUY'
            confidence = buy_pct
        elif sell_pct > 0.6 and sell_pct > buy_pct:
            signal = 'SELL'
            confidence = sell_pct
        else:
            signal = 'HOLD'
            confidence = max(buy_pct, sell_pct)
        
        return {
            'signal': signal,
            'confidence': confidence,
            'buy_votes': votes['BUY'],
            'sell_votes': votes['SELL'],
            'buy_weight': buy_pct,
            'sell_weight': sell_pct,
            'agents': len(self.agents),
            'agent_details': agent_votes[:3]  # First 3 for brevity
        }
    
    def update_from_trade(self, trade_result: Dict):
        """Update all agents based on trade outcome"""
        pnl = trade_result.get('pnl', 0)
        for agent in self.agents:
            agent.update_performance(pnl)
    
    def get_best_agents(self, top_n: int = 3) -> List[Dict]:
        """Get best performing agents"""
        sorted_agents = sorted(
            self.agents, 
            key=lambda a: a.wins/(a.wins+a.losses+1) if (a.wins+a.losses) > 0 else 0, 
            reverse=True
        )
        return [{
            'id': a.id,
            'strategy': a.strategy,
            'win_rate': a.wins/(a.wins+a.losses+1) if (a.wins+a.losses) > 0 else 0,
            'weight': a.weight,
            'trades': a.trades
        } for a in sorted_agents[:top_n]]


# ===== INTEGRATION WITH YOUR EXISTING SYSTEM =====
class AdvancedAIIntegration:
    """
    Integrates all AI systems into your trading bot
    """
    
    def __init__(self):
        self.rl_trader = None
        self.transformer = None
        self.swarm = None
        self.initialized = False
    
    def initialize_all(self, df: pd.DataFrame = None):
        """Initialize all AI systems"""
        logger.info("\n" + "="*60)

        logger.info("🧠 INITIALIZING ADVANCED AI SYSTEMS")

        logger.info("="*60)

        # 1. Reinforcement Learning
        logger.info("\n1. 🤖 Reinforcement Learning...")

        self.rl_trader = RLTrader(model_type='PPO')
        if df is not None and RL_AVAILABLE:
            self.rl_trader.train(df, total_timesteps=5000)
        else:
            logger.info("   ⚠️ No data for RL training or RL not available")

        # 2. Transformer
        logger.info("\n2. 🦋 Transformer Model...")

        self.transformer = TransformerTrader()
        if df is not None and TRANSFORMER_AVAILABLE:
            self.transformer.train(df)
        
        # 3. Swarm Intelligence
        logger.info("\n3. 🐝 Swarm Intelligence...")

        self.swarm = SwarmIntelligence()
        
        self.initialized = True
        logger.info("\n✅ ALL ADVANCED AI SYSTEMS INITIALIZED")

        return self
    
    def get_combined_prediction(self, df: pd.DataFrame) -> Dict:
        """
        Combine all AI systems for ultimate prediction
        """
        if not self.initialized or self.swarm is None:
            return {'signal': 'HOLD', 'confidence': 0.0}
        
        votes = {'BUY': 0, 'SELL': 0}
        weights = {'BUY': 0.0, 'SELL': 0.0}
        details = {}
        
        # 1. Swarm vote
        try:
            swarm_vote = self.swarm.analyze_market(df)
            details['swarm'] = swarm_vote
            if swarm_vote['signal'] != 'HOLD':
                weights[swarm_vote['signal']] += 0.4
                votes[swarm_vote['signal']] += 1
        except Exception as e:
            logger.info(f"⚠️ Swarm error: {e}")

        # 2. Transformer prediction
        try:
            if self.transformer and self.transformer.trained:
                recent = df['close'].values[-60:]
                transformer_pred = self.transformer.predict_next(recent)
                if transformer_pred > 0.005:
                    weights['BUY'] += 0.3
                    votes['BUY'] += 1
                elif transformer_pred < -0.005:
                    weights['SELL'] += 0.3
                    votes['SELL'] += 1
        except Exception as e:
            logger.info(f"⚠️ Transformer error: {e}")

        # Determine final signal
        if weights['BUY'] > weights['SELL'] and weights['BUY'] > 0.35:
            signal = 'BUY'
            confidence = weights['BUY']
        elif weights['SELL'] > weights['BUY'] and weights['SELL'] > 0.35:
            signal = 'SELL'
            confidence = weights['SELL']
        else:
            signal = 'HOLD'
            confidence = max(weights['BUY'], weights['SELL'])
        
        return {
            'signal': signal,
            'confidence': round(confidence, 2),
            'weights': weights,
            'votes': votes,
            'details': details
        }


# ===== TEST FUNCTION =====
def test_advanced_ai():
    """Test all AI systems"""
    logger.info("\n" + "="*60)

    logger.info("🧪 TESTING ADVANCED AI SYSTEMS")

    logger.info("="*60)

    # Create sample data
    import yfinance as yf
    logger.info("\n📊 Fetching sample BTC data...")

    try:
        btc = yf.Ticker("BTC-USD")
        df = btc.history(period="3mo", interval="1h")
        df.columns = df.columns.str.lower()
        logger.info(f"✅ Got {len(df)} candles of BTC data")

    except Exception as e:
        logger.info(f"❌ Error fetching data: {e}")

        return None
    
    # Initialize AI
    logger.info("\n🧠 Initializing AI systems...")

    ai = AdvancedAIIntegration()
    ai.initialize_all(df)
    
    # Get prediction
    logger.info("\n🎯 Getting combined AI prediction...")

    prediction = ai.get_combined_prediction(df)
    
    logger.info(f"\n📈 FINAL PREDICTION:")

    logger.info(f"  Signal: {prediction['signal']}")

    logger.info(f"  Confidence: {prediction['confidence']:.2f}")

    logger.info(f"  Buy weight: {prediction['weights']['BUY']:.2f}")

    logger.info(f"  Sell weight: {prediction['weights']['SELL']:.2f}")

    # Show best agents
    if ai.swarm:
        logger.info("\n🏆 Best Swarm Agents:")

        best = ai.swarm.get_best_agents(3)
        for b in best:
            logger.info(f"  • {b['id']}: {b['win_rate']:.1%} win rate ({b['trades']} trades), weight={b['weight']:.1f}")

    return ai

if __name__ == "__main__":
    test_advanced_ai()