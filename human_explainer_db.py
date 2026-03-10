"""
🤖 HUMAN-LIKE EXPLANATIONS WITH DATABASE STORAGE
Now with PostgreSQL persistence!
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from services.personality_service import PersonalityDatabase


class DatabasePersonality:
    """Personality that remembers everything in database"""
    
    def __init__(self, bot_name="Robbie"):
        self.name = bot_name
        self.db = PersonalityDatabase()
        self.current_mood = self._get_current_mood()
    
    def _get_current_mood(self):
        """Get current mood from database"""
        report = self.db.get_personality_report()
        return {
            'name': report['current_mood'],
            'emoji': report['mood_emoji'],
            'description': self._mood_description(report['current_mood'], report['stats']),
            'stats': report['stats']
        }
    
    def _mood_description(self, mood: str, stats: Dict) -> str:
        """Generate description based on mood"""
        descriptions = {
            'euphoric': f"ON FIRE! {stats['consecutive_wins']} wins in a row! 🔥",
            'confident': f"Feeling good with {stats['last_10_wins']}/10 wins lately",
            'cautious': "Taking it slow after some losses",
            'shaken': f"Oof, {stats['consecutive_losses']} losses in a row. Being careful",
            'on_fire': f"{stats['last_10_wins']}/10 wins! Let's go!",
            'rich': f"Up ${stats['last_10_pnl']:.0f} lately! 💰",
            'grumpy': "Market's being difficult",
            'neutral': "Just another trading day"
        }
        return descriptions.get(mood, "Ready to trade!")
    
    def remember_trade(self, trade_result: Dict):
        """Store trade in database"""
        self.db.record_trade_in_diary(trade_result)
        self.current_mood = self._get_current_mood()  # Update mood
    
    def get_historical_context(self, asset: str, setup_type: str) -> Optional[str]:
        """Get historical context from database"""
        return self.db.get_historical_context(asset, setup_type)
    
    def get_memorable_moment(self, asset: str) -> Optional[str]:
        """Get a memorable moment for this asset"""
        report = self.db.get_personality_report()
        for moment in report['memorable_moments']:
            if moment['asset'] == asset:
                date = moment['date']
                if moment['is_win']:
                    return f"Remember that huge win on {date}? (+${moment['pnl']:.0f})"
                else:
                    return f"Ugh, remember the {date} disaster? (-${abs(moment['pnl']):.0f})"
        return None


class DatabaseExplainer:
    """Explainer that uses database for memory"""
    
    def __init__(self, trading_system):
        self.bot = trading_system
        self.personality = DatabasePersonality("Robbie")
        self.db = PersonalityDatabase()
        
        # Personality traits from DB
        report = self.db.get_personality_report()
        self.traits = report['traits']
        
        # Phrases (still in code, not DB)
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

    def get_market_narrative(self, asset: str, current_price: float) -> str:
        """
        Generate a narrative connecting news events to price moves
        Uses YOUR existing data sources!
        """
        narratives = []
        
        # ===== 1. CHECK FOR GEOPOLITICAL EVENTS =====
        geopolitical_keywords = ['iran', 'israel', 'russia', 'ukraine', 'china', 'middle east']
        
        if hasattr(self.bot, 'sentiment_analyzer'):
            articles = self.bot.sentiment_analyzer.news_integrator.fetch_all_sources()
            
            for article in articles[:30]:  # Check last 30 articles
                title = article['title'].lower()
                
                # Check for geopolitical events
                for keyword in geopolitical_keywords:
                    if keyword in title:
                        # Oil-related assets
                        if asset in ['CL=F', 'WTI/USD', 'XAU/USD', 'GC=F'] and any(x in title for x in ['oil', 'crude', 'energy']):
                            narratives.append(f"🛢️ {article['title']}")
                            break
                        
                        # Gold-related assets
                        elif asset in ['XAU/USD', 'GC=F', 'SI=F'] and any(x in title for x in ['gold', 'silver', 'safe']):
                            narratives.append(f"🏆 {article['title']}")
                            break
                        
                        # Forex
                        elif '/' in asset and any(x in title for x in ['dollar', 'fed', 'currency']):
                            narratives.append(f"💵 {article['title']}")
                            break
                
                if narratives:
                    break
        
        # ===== 2. CHECK FOR WHALE ALERTS (CRYPTO) =====
        if not narratives and asset in ['BTC-USD', 'ETH-USD', 'SOL-USD']:
            try:
                from whale_alert_manager import WhaleAlertManager
                whales = WhaleAlertManager()
                alerts = whales.get_alerts(min_value_usd=5000000)
                
                for alert in alerts[:3]:
                    if alert.get('symbol') in asset:
                        value_m = alert['value_usd'] / 1_000_000
                        narratives.append(f"🐋 Whale Alert: {alert['title']} (${value_m:.1f}M)")
                        break
            except:
                pass
        
        # ===== 3. CHECK ECONOMIC CALENDAR =====
        if not narratives and hasattr(self.bot, 'market_calendar'):
            try:
                events = self.bot.market_calendar.get_high_impact_events(days=3)
                for event in events:
                    event_name = event['event'].lower()
                    
                    # Connect events to assets
                    if 'fed' in event_name and '/' in asset:
                        narratives.append(f"📅 Fed meeting in {event['days']} days - markets pricing in rate expectations")
                        break
                    elif 'cpi' in event_name and asset in ['XAU/USD', 'GC=F', '^GSPC']:
                        narratives.append(f"📅 CPI report in {event['days']} days - inflation data affects all markets")
                        break
                    elif 'opec' in event_name and asset in ['CL=F', 'WTI/USD']:
                        narratives.append(f"📅 OPEC meeting in {event['days']} days - production decisions pending")
                        break
            except:
                pass
        
        # ===== 4. GENERIC NARRATIVES BASED ON ASSET TYPE =====
        if not narratives:
            if asset in ['CL=F', 'WTI/USD']:
                narratives.append("🛢️ Oil prices sensitive to Middle East tensions and global demand")
            elif asset in ['XAU/USD', 'GC=F']:
                narratives.append("🏆 Gold responding to dollar strength and safe-haven flows")
            elif asset in ['BTC-USD', 'ETH-USD']:
                narratives.append("📊 Crypto markets reacting to whale activity and ETF flows")
            elif '/' in asset:
                narratives.append("💵 Forex markets driven by interest rate differentials")
        
        return narratives[0] if narratives else ""
    
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
        
        # ADX trend strength
        if 'adx' in df.columns:
            adx = latest['adx']
            if adx > 40:
                reasons.append(f"Strong trend (ADX {adx:.1f}) - momentum is real")
            elif adx > 25:
                reasons.append(f"Trend developing (ADX {adx:.1f})")
            elif adx < 20:
                reasons.append("Market is ranging - no clear trend")
        
        # ML model count
        if prediction and 'model_count' in prediction:
            models = prediction.get('model_count', 10)
            reasons.append(f"{models} different ML models agree on this")
        
        return reasons
    
    def explain_signal(self, asset: str, df: pd.DataFrame, prediction: Dict,
                  sentiment: Dict, news: List, chat_id: str = None) -> str:
        """Generate complete human-like explanation and save to DB"""
        
        # Handle column naming (close vs Close)
        if 'close' in df.columns:
            current_price = df['close'].iloc[-1]
        elif 'Close' in df.columns:
            current_price = df['Close'].iloc[-1]
        else:
            # Try to find any price column
            price_cols = [col for col in df.columns if 'close' in col.lower()]
            if price_cols:
                current_price = df[price_cols[0]].iloc[-1]
            else:
                current_price = 0
        
        direction = prediction.get('direction', 'HOLD')
        confidence = prediction.get('confidence', 0.5) * 100
        confidence_emoji = self._get_confidence_emoji(confidence)
        
        # ===== GET MARKET NARRATIVE FROM NEWS =====
        market_narrative = self.get_market_narrative(asset, current_price)
        # ==========================================
        
        # Get mood from DB
        mood = self.personality.current_mood
        
        # Start building message
        parts = []
        
        # 1. GREETING + MOOD
        greeting = random.choice(self.greetings)
        parts.append(f"{greeting}")
        
        # Add mood if talkative
        if random.random() < self.traits['talkativeness']:
            parts.append(f"I'm feeling **{mood['name']}** {mood['emoji']} - {mood['description']}")
        
        # 2. MARKET NARRATIVE (NEW - connects news to price moves)
        if market_narrative:
            parts.append(f"\n{market_narrative}")
        
        # 3. MAIN SIGNAL
        market_mood = self._get_market_mood(df)
        
        if direction == 'UP':
            phrase = random.choice(self.bullish_phrases)
            parts.append(f"\n{confidence_emoji} **{asset}** is {phrase}!")
        elif direction == 'DOWN':
            phrase = random.choice(self.bearish_phrases)
            parts.append(f"\n{confidence_emoji} **{asset}** is {phrase}.")
        else:
            parts.append(f"\n⚖️ **{asset}** is looking neutral right now.")
        
        # 4. CONFIDENCE
        if confidence > 80:
            parts.append(f"I'm **pretty damn confident** about this ({confidence:.0f}% sure).")
        elif confidence > 65:
            parts.append(f"Decent signal here ({confidence:.0f}% confidence).")
        elif confidence > 50:
            parts.append(f"Not super confident ({confidence:.0f}%) but worth keeping an eye on.")
        else:
            parts.append(f"Honestly, I'm unsure ({confidence:.0f}%) - market's being weird.")
        
        # 5. MARKET MOOD
        parts.append(f"\n📊 **Market vibe:** {market_mood}")
        
        # 6. TECHNICAL REASONS
        reasons = self._get_technical_reasons(df, prediction)
        if reasons:
            parts.append(f"\n**Why I think that:**")
            for reason in reasons[:4]:
                parts.append(f"  • {reason}")
        
        # 7. HISTORICAL CONTEXT (from DB)
        setup_type = "breakout" if "breakout" in str(reasons).lower() else "pullback"
        historical = self.personality.get_historical_context(asset, setup_type)
        if historical:
            parts.append(f"\n📝 **Trading diary:** {historical}")
        
        # 8. MEMORABLE MOMENT (from DB)
        moment = self.personality.get_memorable_moment(asset)
        if moment and random.random() < 0.3:  # 30% chance to mention
            parts.append(f"\n💭 {moment}")
        
        # 9. NEWS HEADLINES (fallback if no narrative found)
        if not market_narrative and news and len(news) > 0:
            parts.append(f"\n📰 **In the news:**")
            for article in news[:2]:
                headline = article.get('title', '')[:80]
                if headline:
                    parts.append(f"  • \"{headline}...\"")
        
        # 10. CURRENT PRICE AND TARGET
        parts.append(f"\n💰 **Current price:** ${current_price:,.2f}")
        
        if 'predicted_price' in prediction:
            target = prediction['predicted_price']
            change = ((target - current_price) / current_price) * 100
            if change > 0:
                parts.append(f"🎯 **Target:** ${target:,.2f} (+{change:.1f}%)")
            else:
                parts.append(f"🎯 **Target:** ${target:,.2f} ({change:.1f}%)")
        
        # 11. ACTIONABLE ADVICE
        if direction == 'UP' and confidence > 70:
            parts.append(f"\n💡 **My advice:** If you're feeling it, this could be a good entry.")
        elif direction == 'DOWN' and confidence > 70:
            parts.append(f"\n💡 **My advice:** Might want to wait or consider shorts if that's your thing.")
        else:
            parts.append(f"\n💡 **My advice:** Probably best to wait and see.")
        
        # 12. SIGN-OFF
        signoffs = [
            f"\nThat's my 2 satoshis! 🪙",
            f"\nCatch you later! 👊",
            f"\nLet's see if it plays out! 🤞",
            f"\n— Your friendly trading bot 🤖",
            f"\nBack to watching charts! 📈",
        ]
        
        if mood['name'] == 'euphoric':
            signoffs.append(f"\nCAN'T STOP WON'T STOP! 🚀")
        elif mood['name'] == 'shaken':
            signoffs.append(f"\nBeing extra cautious today...")
        
        parts.append(random.choice(signoffs))
        
        final_message = "\n".join(parts)
        
        # 13. SAVE TO DATABASE
        try:
            rsi_value = df['rsi'].iloc[-1] if 'rsi' in df.columns else None
            volume_value = df['volume'].iloc[-1] if 'volume' in df.columns else None
            
            self.db.save_explanation({
                'asset': asset,
                'text': final_message,
                'direction': direction,
                'confidence': confidence / 100,
                'rsi': rsi_value,
                'volume': volume_value,
                'news_count': len(news),
                'sentiment': sentiment.get('score', 0) if sentiment else 0,
                'sent_to_telegram': chat_id is not None,
                'chat_id': chat_id
            })
        except Exception as e:
            print(f"Couldn't save explanation to DB: {e}")
        
        return final_message
    
    def close(self):
        """Close database connection"""
        self.db.close()