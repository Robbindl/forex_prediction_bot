"""
🤖 HUMAN-LIKE EXPLANATIONS WITH PERSONALITY
Your bot with memory, mood, and trading diary
"""

import pandas as pd
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path

class TradingPersonality:
    """Gives your bot a personality with memory and mood"""
    
    def __init__(self, bot_name="Robbie"):
        self.name = bot_name
        self.memory_file = Path("trading_diary.json")
        self.diary = self._load_diary()
        self.current_mood = self._calculate_mood()
        
    def _load_diary(self) -> Dict:
        """Load trading diary from file"""
        if self.memory_file.exists():
            with open(self.memory_file, 'r') as f:
                return json.load(f)
        return {
            'trades': [],
            'memorable_days': {},
            'personality_traits': {
                'confidence': 0.7,  # Base confidence
                'cautiousness': 0.5,  # 0=reckless, 1=very cautious
                'optimism': 0.6,      # 0=pessimist, 1=optimist
                'talkativeness': 0.7  # 0=quiet, 1=chatty
            }
        }
    
    def _save_diary(self):
        """Save diary to file"""
        with open(self.memory_file, 'w') as f:
            json.dump(self.diary, f, indent=2)
    
    def _calculate_mood(self) -> Dict:
        """Calculate current mood based on recent performance"""
        recent_trades = self.diary['trades'][-20:] if self.diary['trades'] else []
        
        if not recent_trades:
            return {
                'name': 'excited',
                'emoji': '🤩',
                'description': 'Fresh and ready to trade!',
                'multiplier': 1.0
            }
        
        # Calculate win rate last 10 trades
        last_10 = recent_trades[-10:] if len(recent_trades) >= 10 else recent_trades
        wins = sum(1 for t in last_10 if t.get('pnl', 0) > 0)
        win_rate = wins / len(last_10) if last_10 else 0.5
        
        # Recent P&L trend
        recent_pnl = sum(t.get('pnl', 0) for t in last_10)
        
        if win_rate > 0.7 and recent_pnl > 0:
            return {
                'name': 'euphoric',
                'emoji': '🚀🚀🚀',
                'description': 'ON FIRE! Can\'t lose! 🔥',
                'multiplier': 1.3
            }
        elif win_rate > 0.6 and recent_pnl > 0:
            return {
                'name': 'confident',
                'emoji': '😎',
                'description': 'Feeling good about these setups',
                'multiplier': 1.1
            }
        elif win_rate > 0.5:
            return {
                'name': 'cautious',
                'emoji': '🤔',
                'description': 'Taking it slow, watching closely',
                'multiplier': 0.9
            }
        elif recent_pnl < -100:  # Big losses
            return {
                'name': 'shaken',
                'emoji': '😰',
                'description': 'Oof, that hurt. Being extra careful now',
                'multiplier': 0.6
            }
        else:
            return {
                'name': 'grumpy',
                'emoji': '😤',
                'description': 'Market\'s being difficult right now',
                'multiplier': 0.8
            }
    
    def remember_trade(self, trade_result: Dict):
        """Store trade in diary for future reference"""
        self.diary['trades'].append({
            'asset': trade_result.get('asset'),
            'pnl': trade_result.get('pnl'),
            'exit_reason': trade_result.get('exit_reason'),
            'setup': trade_result.get('setup', 'unknown'),
            'timestamp': datetime.now().isoformat()
        })
        
        # Keep last 100 trades
        if len(self.diary['trades']) > 100:
            self.diary['trades'] = self.diary['trades'][-100:]
        
        self._save_diary()
        self.current_mood = self._calculate_mood()  # Update mood
    
    def remember_memorable_day(self, date: str, description: str):
        """Store memorable trading days (good or bad)"""
        self.diary['memorable_days'][date] = description
        self._save_diary()
    
    def get_historical_context(self, asset: str, current_setup: str) -> Optional[str]:
        """Find similar past setups and remind user"""
        if not self.diary['trades']:
            return None
        
        # Look for similar trades in last 30 days
        similar_trades = []
        cutoff = datetime.now() - timedelta(days=30)
        
        for trade in self.diary['trades'][-50:]:
            if trade['asset'] != asset:
                continue
            
            trade_time = datetime.fromisoformat(trade['timestamp'])
            if trade_time < cutoff:
                continue
            
            # Simple similarity - same asset and similar exit reason
            if trade.get('setup') == current_setup:
                similar_trades.append(trade)
        
        if not similar_trades:
            return None
        
        # Calculate how those trades performed
        wins = [t for t in similar_trades if t.get('pnl', 0) > 0]
        win_rate = len(wins) / len(similar_trades) if similar_trades else 0
        
        if win_rate > 0.7:
            return f"This setup worked well last time ({len(wins)}/{len(similar_trades)} wins)"
        elif win_rate > 0.5:
            return f"We've seen this {len(similar_trades)} times before - mixed results"
        else:
            # Find a specific memorable example
            for trade in similar_trades[:3]:
                if trade.get('pnl', 0) < -50:  # Big loss
                    date = datetime.fromisoformat(trade['timestamp']).strftime('%A')
                    return f"Careful - last time we tried this on {date}, we got burned (-${abs(trade['pnl']):.0f})"
        
        return None


class HumanExplainer:
    """Turns technical signals into friendly explanations"""
    
    def __init__(self, trading_system):
        self.bot = trading_system
        self.personality = TradingPersonality("Robbie")  # Your name from logs!
        self.greetings = [
            "Hey Robbie! 👋",
            f"Yo {self.personality.name}!",
            f"Morning {self.personality.name}! ☕",
            "Quick update for you:",
            "Check this out:",
        ]
        
        self.bullish_phrases = [
            "looking bullish", "ready to pump", "showing strength", 
            "on the move", "looking juicy", "getting interesting",
            "green across the board", "buyers are stepping in"
        ]
        
        self.bearish_phrases = [
            "looking bearish", "showing weakness", "might dip",
            "under pressure", "sellers are in control", "looking shaky",
            "red flags popping up"
        ]
    
    def _get_confidence_emoji(self, confidence: float) -> str:
        """Return emoji based on confidence level"""
        if confidence >= 90:
            return "🚀🚀🚀"
        elif confidence >= 80:
            return "🚀🚀"
        elif confidence >= 70:
            return "🚀"
        elif confidence >= 60:
            return "🤔"
        else:
            return "🤷"
    
    def _get_market_mood(self, df: pd.DataFrame) -> str:
        """Detect overall market mood from data"""
        try:
            latest = df.iloc[-1]
            
            # Count green vs red candles
            last_10 = df.tail(10)
            green_candles = sum(1 for _, row in last_10.iterrows() if row['close'] > row['open'])
            red_candles = 10 - green_candles
            
            # Check volatility
            if 'atr' in df.columns:
                atr_pct = (latest['atr'] / latest['close']) * 100
            else:
                atr_pct = 2.0
            
            # Determine mood
            if green_candles >= 7:
                if atr_pct > 3:
                    return "excited and volatile! Lots of green but it's wild"
                else:
                    return "euphoric! Everything's green, feels like a party"
            elif green_candles >= 5:
                return "positive but calm - steady green"
            elif red_candles >= 7:
                if atr_pct > 3:
                    return "scary right now - red candles everywhere with high volatility"
                else:
                    return "gloomy - mostly red, be careful"
            else:
                return "choppy - back and forth, no clear direction"
                
        except:
            return "neutral - nothing special"
    
    def explain_signal(self, asset: str, df: pd.DataFrame, prediction: Dict,
                      sentiment: Dict, news: List) -> str:
        """Generate complete human-like explanation"""
        
        current_price = df['close'].iloc[-1]
        direction = prediction.get('direction', 'HOLD')
        confidence = prediction.get('confidence', 0.5) * 100
        confidence_emoji = self._get_confidence_emoji(confidence)
        
        # Start building message
        parts = []
        
        # 1. GREETING + MOOD
        mood = self.personality.current_mood
        greeting = random.choice(self.greetings)
        parts.append(f"{greeting}")
        
        if random.random() < mood['multiplier'] * 0.3:  # Talkative when confident
            parts.append(f"I'm feeling **{mood['name']}** {mood['emoji']} today - {mood['description']}")
        
        # 2. MAIN SIGNAL
        market_mood = self._get_market_mood(df)
        
        if direction == 'UP':
            phrase = random.choice(self.bullish_phrases)
            parts.append(f"\n{confidence_emoji} **{asset}** is {phrase}!")
        elif direction == 'DOWN':
            phrase = random.choice(self.bearish_phrases)
            parts.append(f"\n{confidence_emoji} **{asset}** is {phrase}.")
        else:
            parts.append(f"\n⚖️ **{asset}** is looking neutral right now.")
        
        # 3. CONFIDENCE
        if confidence > 80:
            parts.append(f"I'm **pretty damn confident** about this ({confidence:.0f}% sure).")
        elif confidence > 65:
            parts.append(f"Decent signal here ({confidence:.0f}% confidence).")
        elif confidence > 50:
            parts.append(f"Not super confident ({confidence:.0f}%) but worth keeping an eye on.")
        else:
            parts.append(f"Honestly, I'm unsure ({confidence:.0f}%) - market's being weird.")
        
        # 4. MARKET MOOD
        parts.append(f"\n📊 **Market vibe:** {market_mood}")
        
        # 5. THE "WHY" - TECHNICAL REASONS
        reasons = self._get_technical_reasons(df, prediction)
        if reasons:
            parts.append(f"\n**Why I think that:**")
            for reason in reasons[:4]:  # Top 4 reasons
                parts.append(f"  • {reason}")
        
        # 6. HISTORICAL CONTEXT (Trading Diary)
        setup_type = "breakout" if "breakout" in str(reasons).lower() else "pullback"
        historical = self.personality.get_historical_context(asset, setup_type)
        if historical:
            parts.append(f"\n📝 **Trading diary:** {historical}")
        
        # 7. NEWS HEADLINES
        if news and len(news) > 0:
            parts.append(f"\n📰 **In the news:**")
            for article in news[:2]:
                headline = article.get('title', '')[:80]
                if headline:
                    parts.append(f"  • \"{headline}...\"")
        
        # 8. REMEMBER MEMORABLE MOMENTS
        if confidence > 85 and direction == 'UP':
            # Check if we've seen this before
            for trade in self.personality.diary['trades'][-20:]:
                if trade.get('asset') == asset and trade.get('pnl', 0) > 100:
                    date = datetime.fromisoformat(trade['timestamp']).strftime('%A')
                    parts.append(f"\n💭 Reminds me of last {date} when we caught that nice run!")
                    break
        
        # 9. CURRENT PRICE AND TARGET
        parts.append(f"\n💰 **Current price:** ${current_price:,.2f}")
        
        if 'predicted_price' in prediction:
            target = prediction['predicted_price']
            change = ((target - current_price) / current_price) * 100
            if change > 0:
                parts.append(f"🎯 **Target:** ${target:,.2f} (+{change:.1f}%)")
            else:
                parts.append(f"🎯 **Target:** ${target:,.2f} ({change:.1f}%)")
        
        # 10. ACTIONABLE ADVICE
        if direction == 'UP' and confidence > 70:
            parts.append(f"\n💡 **My advice:** If you're feeling it, this could be a good entry.")
        elif direction == 'DOWN' and confidence > 70:
            parts.append(f"\n💡 **My advice:** Might want to wait or consider shorts if that's your thing.")
        else:
            parts.append(f"\n💡 **My advice:** Probably best to wait and see.")
        
        # 11. SIGN-OFF WITH PERSONALITY
        signoffs = [
            f"\nThat's my 2 satoshis! 🪙",
            f"\nCatch you later! 👊",
            f"\nLet's see if it plays out! 🤞",
            f"\n— Your friendly trading bot 🤖",
            f"\nBack to watching charts! 📈",
            f"\n*goes back to analyzing candles* 🕯️"
        ]
        
        # Add personality-based signoff
        if mood['name'] == 'euphoric':
            signoffs.append(f"\nCAN'T STOP WON'T STOP! 🚀")
        elif mood['name'] == 'shaken':
            signoffs.append(f"\nBeing extra cautious today...")
        
        parts.append(random.choice(signoffs))
        
        return "\n".join(parts)
    
    def _get_technical_reasons(self, df: pd.DataFrame, prediction: Dict) -> List[str]:
        """Extract human-readable reasons from indicators"""
        reasons = []
        latest = df.iloc[-1]
        
        # RSI reasons
        if 'rsi' in df.columns:
            rsi = latest['rsi']
            prev_rsi = df['rsi'].iloc[-2] if len(df) > 1 else rsi
            
            if rsi < 30:
                reasons.append(f"RSI is **oversold** at {rsi:.1f} (usually means a bounce is coming)")
            elif rsi < 40:
                reasons.append(f"RSI is recovering from {prev_rsi:.1f} to {rsi:.1f}")
            elif rsi > 70:
                reasons.append(f"RSI is **overbought** at {rsi:.1f} - might see a pullback")
            elif rsi > 60:
                reasons.append(f"RSI showing strength at {rsi:.1f}")
            
            if abs(rsi - prev_rsi) > 10:
                direction = "jumped" if rsi > prev_rsi else "dropped"
                reasons.append(f"RSI just {direction} from {prev_rsi:.1f} to {rsi:.1f}")
        
        # Volume reasons
        if 'volume' in df.columns and df['volume'].sum() > 0:
            current_vol = latest['volume']
            avg_vol = df['volume'].rolling(20).mean().iloc[-1]
            if avg_vol > 0:
                vol_ratio = current_vol / avg_vol
                if vol_ratio > 2.5:
                    reasons.append(f"Volume is **MASSIVE** ({vol_ratio:.1f}x normal) - big money moving")
                elif vol_ratio > 1.8:
                    reasons.append(f"Strong volume ({vol_ratio:.1f}x average) - confirms the move")
                elif vol_ratio > 1.3:
                    reasons.append(f"Above-average volume ({vol_ratio:.1f}x normal)")
        
        # Moving averages
        if 'sma_20' in df.columns and 'sma_50' in df.columns:
            sma20 = latest['sma_20']
            sma50 = latest['sma_50']
            price = latest['close']
            
            if price > sma20 and sma20 > sma50:
                reasons.append("Price is above both 20 & 50 MAs - bulls in control")
            elif price < sma20 and sma20 < sma50:
                reasons.append("Price below both MAs - bears have the upper hand")
            elif price > sma20 and sma20 < sma50:
                reasons.append("Golden cross forming? 20MA crossing above 50MA soon")
        
        # Bollinger Bands
        if 'bb_upper' in df.columns:
            bb_upper = latest['bb_upper']
            bb_lower = latest['bb_lower']
            bb_middle = latest.get('bb_middle', (bb_upper + bb_lower) / 2)
            price = latest['close']
            
            if price > bb_upper * 0.99:
                reasons.append("Testing upper Bollinger Band - possible breakout incoming")
            elif price < bb_lower * 1.01:
                reasons.append("Bouncing off lower Bollinger Band - could reverse up")
            elif price < bb_middle and price > bb_lower:
                reasons.append("Price below middle BB - still in lower range")
            elif price > bb_middle and price < bb_upper:
                reasons.append("Price above middle BB - trending up")
        
        # MACD
        if 'macd' in df.columns and 'macd_signal' in df.columns:
            macd = latest['macd']
            signal = latest['macd_signal']
            prev_macd = df['macd'].iloc[-2] if len(df) > 1 else macd
            prev_signal = df['macd_signal'].iloc[-2] if len(df) > 1 else signal
            
            if macd > signal and prev_macd <= prev_signal:
                reasons.append("MACD just crossed above signal line - bullish momentum")
            elif macd < signal and prev_macd >= prev_signal:
                reasons.append("MACD crossed below signal - bearish momentum")
        
        # ADX trend strength
        if 'adx' in df.columns:
            adx = latest['adx']
            if adx > 40:
                reasons.append(f"Strong trend (ADX {adx:.1f}) - momentum is real")
            elif adx > 25:
                reasons.append(f"Trend developing (ADX {adx:.1f})")
            elif adx < 20:
                reasons.append("Market is ranging - no clear trend")
        
        # Support/Resistance
        if 'bb_lower' in df.columns:
            if abs(latest['close'] - bb_lower) / bb_lower < 0.01:
                reasons.append("Price at support level - could bounce")
        
        # ML model count
        if prediction and 'model_count' in prediction:
            models = prediction.get('model_count', 10)
            reasons.append(f"{models} different ML models agree on this")
        
        return reasons


# Example usage in your trading system:
def get_human_signal(bot, asset="BTC-USD"):
    """Get human-readable signal for any asset"""
    
    # Fetch data
    df = bot.fetch_historical_data(asset, days=3, interval='15m')
    df = bot.add_technical_indicators(df)
    
    # Get ML prediction
    prediction = bot.predictor.predict_next(df)
    
    # Get sentiment
    sentiment = {}
    if hasattr(bot, 'sentiment_analyzer'):
        sentiment = bot.sentiment_analyzer.get_comprehensive_sentiment('crypto')
    
    # Get recent news
    news = []
    if hasattr(bot, 'sentiment_analyzer') and hasattr(bot.sentiment_analyzer, 'news_integrator'):
        news = bot.sentiment_analyzer.news_integrator.fetch_by_symbol(asset, limit=3)
    
    # Generate human explanation
    explainer = HumanExplainer(bot)
    message = explainer.explain_signal(asset, df, prediction, sentiment, news)
    
    return message