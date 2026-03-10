"""
Personality Service - Bot memory, mood, and human explanations
Combine this into ONE file
"""

from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import random
import pandas as pd

from models.trade_models import TradingDiary, BotPersonality, MemorableMoments, HumanExplanations, Trade
from config.database import SessionLocal


class PersonalityDatabase:
    """Handles all personality-related database operations"""
    
    def __init__(self):
        self.session = SessionLocal()
        self._ensure_personality_exists()
    
    def _ensure_personality_exists(self):
        """Create default personality if none exists"""
        personality = self.session.query(BotPersonality).first()
        if not personality:
            personality = BotPersonality(
                bot_name='Robbie',
                base_confidence=0.7,
                cautiousness=0.5,
                optimism=0.6,
                talkativeness=0.7,
                current_mood='excited',
                mood_emoji='🤩'
            )
            self.session.add(personality)
            self.session.commit()
    
    def record_trade_in_diary(self, trade_data: Dict) -> int:
        """Store trade in diary for future reference"""
        
        # Find the trade if it exists
        trade_id = trade_data.get('trade_id')
        trade = None
        if trade_id:
            trade = self.session.query(Trade).filter(Trade.trade_id == trade_id).first()
        
        # Create diary entry
        diary_entry = TradingDiary(
            asset=trade_data.get('asset'),
            trade_id=trade_id,
            setup_type=trade_data.get('setup_type', 'unknown'),
            pnl=trade_data.get('pnl', 0),
            exit_reason=trade_data.get('exit_reason'),
            entry_price=trade_data.get('entry_price'),
            exit_price=trade_data.get('exit_price'),
            confidence=trade_data.get('confidence', 0.5),
            rsi_at_entry=trade_data.get('rsi_at_entry'),
            volume_ratio=trade_data.get('volume_ratio'),
            market_regime=trade_data.get('market_regime'),
            notes=trade_data.get('notes', {})
        )
        
        self.session.add(diary_entry)
        self.session.commit()
        
        # Update personality stats
        self._update_personality_stats(trade_data)
        
        # Check if this is memorable
        self._check_memorable_moment(trade_data)
        
        return diary_entry.id
    
    def _update_personality_stats(self, trade_data: Dict):
        """Update bot's personality based on trade results"""
        personality = self.session.query(BotPersonality).first()
        if not personality:
            return
        
        pnl = trade_data.get('pnl', 0)
        is_win = pnl > 0
        
        # Update consecutive counts
        if is_win:
            personality.consecutive_wins += 1
            personality.consecutive_losses = 0
        else:
            personality.consecutive_losses += 1
            personality.consecutive_wins = 0
        
        # Update last 10 trades stats
        recent_trades = self.session.query(TradingDiary)\
            .order_by(TradingDiary.created_at.desc())\
            .limit(10).all()
        
        if recent_trades:
            wins = sum(1 for t in recent_trades if t.pnl and t.pnl > 0)
            personality.last_10_wins = wins
            personality.last_10_pnl = sum(t.pnl for t in recent_trades if t.pnl)
        
        personality.total_trades_remembered += 1
        
        # Update mood based on recent performance
        personality.current_mood, personality.mood_emoji = self._calculate_mood(personality)
        
        self.session.commit()
    
    def _calculate_mood(self, personality: BotPersonality) -> tuple:
        """Calculate bot's mood based on recent performance"""
        
        if personality.consecutive_wins >= 5:
            return 'euphoric', '🚀🚀🚀'
        elif personality.consecutive_wins >= 3:
            return 'confident', '😎'
        elif personality.consecutive_losses >= 3:
            return 'shaken', '😰'
        elif personality.consecutive_losses >= 2:
            return 'cautious', '🤔'
        elif personality.last_10_wins >= 8:
            return 'on_fire', '🔥'
        elif personality.last_10_pnl and personality.last_10_pnl > 500:
            return 'rich', '🤑'
        elif personality.last_10_pnl and personality.last_10_pnl < -200:
            return 'grumpy', '😤'
        else:
            return 'neutral', '😐'
    
    def _check_memorable_moment(self, trade_data: Dict):
        """Check if trade is memorable (big win/loss or unusual setup)"""
        pnl = abs(trade_data.get('pnl', 0))
        
        # Is it memorable?
        is_memorable = False
        title = ""
        
        if pnl > 500:  # Big win
            is_memorable = True
            title = f"The Great {trade_data.get('asset')} Pump"
        elif pnl < -300:  # Big loss
            is_memorable = True
            title = f"The {trade_data.get('asset')} Disaster"
        elif trade_data.get('volume_ratio', 1) > 3:  # Huge volume
            is_memorable = True
            title = f"Whale Watching: {trade_data.get('asset')}"
        
        if is_memorable:
            moment = MemorableMoments(
                moment_date=datetime.now(),
                title=title,
                description=trade_data.get('reason', ''),
                asset=trade_data.get('asset'),
                pnl=trade_data.get('pnl'),
                is_win=trade_data.get('pnl', 0) > 0,
                is_memorable=True,
                tags={
                    'setup': trade_data.get('setup_type'),
                    'volume_ratio': trade_data.get('volume_ratio'),
                    'rsi': trade_data.get('rsi_at_entry')
                }
            )
            self.session.add(moment)
            self.session.commit()
    
    def find_similar_setups(self, asset: str, setup_type: str, days_back: int = 30) -> List[Dict]:
        """Find similar trading setups in history"""
        cutoff = datetime.now() - timedelta(days=days_back)
        
        similar = self.session.query(TradingDiary)\
            .filter(
                TradingDiary.asset == asset,
                TradingDiary.setup_type == setup_type,
                TradingDiary.created_at >= cutoff
            )\
            .order_by(TradingDiary.created_at.desc())\
            .all()
        
        results = []
        for s in similar[:10]:
            results.append({
                'date': s.created_at.strftime('%Y-%m-%d %H:%M'),
                'pnl': float(s.pnl) if s.pnl else 0,
                'was_win': s.pnl and s.pnl > 0,
                'confidence': float(s.confidence) if s.confidence else 0,
                'exit_reason': s.exit_reason
            })
        
        return results
    
    def get_historical_context(self, asset: str, setup_type: str) -> Optional[str]:
        """Generate human-readable historical context"""
        similar = self.find_similar_setups(asset, setup_type, days_back=30)
        
        if not similar:
            return None
        
        wins = sum(1 for s in similar if s['was_win'])
        total = len(similar)
        win_rate = wins / total if total > 0 else 0
        
        if win_rate > 0.7:
            return f"This setup worked well last time ({wins}/{total} wins)"
        elif win_rate > 0.5:
            return f"We've seen this {total} times before - mixed results"
        else:
            # Find a specific memorable loss
            losses = [s for s in similar if not s['was_win']]
            if losses:
                worst = min(losses, key=lambda x: x['pnl'])
                day = worst['date'].split(' ')[0]
                return f"Careful - last time we tried this on {day}, we lost ${abs(worst['pnl']):.0f}"
        
        return None
    
    def save_explanation(self, explanation_data: Dict) -> int:
        """Save explanation to database for analytics"""
        explanation = HumanExplanations(
            asset=explanation_data.get('asset'),
            explanation_text=explanation_data.get('text')[:4000],
            direction=explanation_data.get('direction'),
            confidence=explanation_data.get('confidence', 0),
            rsi_value=explanation_data.get('rsi'),
            volume_value=explanation_data.get('volume'),
            news_count=explanation_data.get('news_count', 0),
            sentiment_score=explanation_data.get('sentiment', 0),
            sent_to_telegram=explanation_data.get('sent_to_telegram', False),
            telegram_chat_id=explanation_data.get('chat_id')
        )
        self.session.add(explanation)
        self.session.commit()
        return explanation.id
    
    def get_personality_report(self) -> Dict:
        """Get full personality report"""
        personality = self.session.query(BotPersonality).first()
        
        # Get recent trades stats
        last_week = datetime.now() - timedelta(days=7)
        weekly_trades = self.session.query(TradingDiary)\
            .filter(TradingDiary.created_at >= last_week).count()
        
        weekly_wins = self.session.query(TradingDiary)\
            .filter(
                TradingDiary.created_at >= last_week,
                TradingDiary.pnl > 0
            ).count()
        
        # Get memorable moments
        moments = self.session.query(MemorableMoments)\
            .order_by(MemorableMoments.moment_date.desc())\
            .limit(5).all()
        
        return {
            'name': personality.bot_name if personality else 'Robbie',
            'current_mood': personality.current_mood if personality else 'neutral',
            'mood_emoji': personality.mood_emoji if personality else '😐',
            'traits': {
                'confidence': float(personality.base_confidence) if personality else 0.7,
                'cautiousness': float(personality.cautiousness) if personality else 0.5,
                'optimism': float(personality.optimism) if personality else 0.6,
                'talkativeness': float(personality.talkativeness) if personality else 0.7,
            },
            'stats': {
                'total_trades_remembered': personality.total_trades_remembered if personality else 0,
                'consecutive_wins': personality.consecutive_wins if personality else 0,
                'consecutive_losses': personality.consecutive_losses if personality else 0,
                'last_10_wins': personality.last_10_wins if personality else 0,
                'last_10_pnl': float(personality.last_10_pnl) if personality and personality.last_10_pnl else 0,
                'weekly_trades': weekly_trades,
                'weekly_win_rate': (weekly_wins / weekly_trades * 100) if weekly_trades > 0 else 0
            },
            'memorable_moments': [
                {
                    'title': m.title,
                    'date': m.moment_date.strftime('%Y-%m-%d'),
                    'asset': m.asset,
                    'pnl': float(m.pnl) if m.pnl else 0,
                    'is_win': m.is_win
                }
                for m in moments
            ]
        }
    
    def close(self):
        """Close database session"""
        self.session.close()


class DatabaseExplainer:
    """Explainer that uses database for memory"""
    
    def __init__(self, trading_system):
        self.bot = trading_system
        self.db = PersonalityDatabase()
        self.report = self.db.get_personality_report()
        
        # Phrases
        self.greetings = [
            "Hey Robbie! 👋",
            f"Yo {self.report['name']}!",
            f"Morning {self.report['name']}! ☕",
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
        
        current_price = df['close'].iloc[-1]
        direction = prediction.get('direction', 'HOLD')
        confidence = prediction.get('confidence', 0.5) * 100
        confidence_emoji = self._get_confidence_emoji(confidence)
        
        # Start building message
        parts = []
        
        # 1. GREETING
        greeting = random.choice(self.greetings)
        parts.append(f"{greeting}")
        
        # 2. MAIN SIGNAL
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
        
        # 4. TECHNICAL REASONS
        reasons = self._get_technical_reasons(df, prediction)
        if reasons:
            parts.append(f"\n**Why I think that:**")
            for reason in reasons[:4]:
                parts.append(f"  • {reason}")
        
        # 5. HISTORICAL CONTEXT (from DB)
        setup_type = "breakout" if "breakout" in str(reasons).lower() else "pullback"
        historical = self.db.get_historical_context(asset, setup_type)
        if historical:
            parts.append(f"\n📝 **Trading diary:** {historical}")
        
        # 6. NEWS HEADLINES
        if news and len(news) > 0:
            parts.append(f"\n📰 **In the news:**")
            for article in news[:2]:
                headline = article.get('title', '')[:80]
                if headline:
                    parts.append(f"  • \"{headline}...\"")
        
        # 7. CURRENT PRICE AND TARGET
        parts.append(f"\n💰 **Current price:** ${current_price:,.2f}")
        
        if 'predicted_price' in prediction:
            target = prediction['predicted_price']
            change = ((target - current_price) / current_price) * 100
            if change > 0:
                parts.append(f"🎯 **Target:** ${target:,.2f} (+{change:.1f}%)")
            else:
                parts.append(f"🎯 **Target:** ${target:,.2f} ({change:.1f}%)")
        
        # 8. SIGN-OFF
        signoffs = [
            f"\nThat's my 2 satoshis! 🪙",
            f"\nCatch you later! 👊",
            f"\nLet's see if it plays out! 🤞",
            f"\n— Your friendly trading bot 🤖",
        ]
        parts.append(random.choice(signoffs))
        
        final_message = "\n".join(parts)
        
        # 9. SAVE TO DATABASE
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