"""
Strategy Voting Engine - Combines signals from all your strategies
Now includes news sentiment as a 13th strategy!
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import json
from logger import logger

class StrategyVotingEngine:
    """
    Combines multiple strategies using weighted voting
    Tracks performance of each strategy over time
    Now includes news sentiment!
    """
    
    def __init__(self, trading_system):
        self.trading_system = trading_system
        self.strategy_weights = {}
        self.strategy_performance = {}
        self.vote_history = []
        
        # Initialize all strategies with equal weights
        for strategy_name in trading_system.strategies.keys():
            self.strategy_weights[strategy_name] = 1.0
            self.strategy_performance[strategy_name] = {
                'signals': 0,
                'trades': 0,
                'wins': 0,
                'losses': 0,
                'total_pnl': 0.0,
                'win_rate': 0.0
            }
        
        # ===== ADD NEWS SENTIMENT AS A STRATEGY =====
        self.strategy_weights['news_sentiment'] = 0.8  # Slightly lower weight than technicals
        self.strategy_performance['news_sentiment'] = {
            'signals': 0,
            'trades': 0,
            'wins': 0,
            'losses': 0,
            'total_pnl': 0.0,
            'win_rate': 0.0
        }
        # ============================================
        
        logger.info(f"Voting Engine initialized with {len(self.strategy_weights)} strategies (including news sentiment)")
    
    def get_all_signals(self, df: pd.DataFrame) -> Dict[str, Dict]:
        """
        Get signals from ALL strategies including news sentiment
        """
        signals = {}
        
        # Get signals from all technical strategies
        for name, strategy_func in self.trading_system.strategies.items():
            try:
                strategy_signals = strategy_func(df)
                if strategy_signals and len(strategy_signals) > 0:
                    # Take the most recent signal
                    latest_signal = strategy_signals[-1]
                    signals[name] = latest_signal
                    self.strategy_performance[name]['signals'] += 1
                    logger.debug(f"Strategy {name} generated signal: {latest_signal.get('signal')}")
            except Exception as e:
                logger.error(f"Strategy {name} error: {e}")
        
        # ===== GET NEWS SENTIMENT SIGNAL =====
        try:
            from sentiment_analyzer import SentimentAnalyzer
            sentiment_analyzer = SentimentAnalyzer()
            
            # Get comprehensive sentiment for general market
            sentiment = sentiment_analyzer.get_comprehensive_sentiment()
            
            # Convert sentiment to trading signal
            news_signal = self._convert_sentiment_to_signal(sentiment)
            if news_signal:
                signals['news_sentiment'] = news_signal
                self.strategy_performance['news_sentiment']['signals'] += 1
                logger.info(f"📰 News Sentiment: {sentiment.get('overall_sentiment', 'Neutral')} ({news_signal['confidence']:.0%})")
        except Exception as e:
            logger.error(f"News sentiment error: {e}")
        # ======================================
        
        logger.debug(f"Total signals generated: {len(signals)}")
        return signals
    
    def _convert_sentiment_to_signal(self, sentiment: Dict) -> Optional[Dict]:
        """
        Convert news sentiment to trading signal
        """
        score = sentiment.get('score', 0)
        interpretation = sentiment.get('overall_sentiment', 'Neutral')
        article_count = sentiment.get('article_count', 0)
        
        # Only generate signals if we have enough articles
        if article_count < 5:
            logger.debug(f"News sentiment skipped: only {article_count} articles")
            return None
        
        # Base confidence on sentiment magnitude and article count
        # More articles = higher confidence
        article_confidence = min(article_count / 20, 0.3)  # Max 30% boost from article count
        base_confidence = min(abs(score) * 1.5, 0.7)  # Max 70% from sentiment
        confidence = min(base_confidence + article_confidence, 0.85)  # Cap at 85%
        
        if interpretation == "Very Bullish":
            logger.debug(f"News sentiment: Very Bullish (confidence: {confidence:.2f})")
            return {
                'signal': 'BUY',
                'confidence': confidence,
                'stop_loss': None,  # Will be calculated by weighted vote
                'take_profit': None,
                'reason': f"News sentiment: {interpretation} ({article_count} articles)",
                'strategy': 'news_sentiment'
            }
        elif interpretation == "Bullish":
            logger.debug(f"News sentiment: Bullish (confidence: {confidence*0.8:.2f})")
            return {
                'signal': 'BUY',
                'confidence': confidence * 0.8,
                'stop_loss': None,
                'take_profit': None,
                'reason': f"News sentiment: {interpretation} ({article_count} articles)",
                'strategy': 'news_sentiment'
            }
        elif interpretation == "Very Bearish":
            logger.debug(f"News sentiment: Very Bearish (confidence: {confidence:.2f})")
            return {
                'signal': 'SELL',
                'confidence': confidence,
                'stop_loss': None,
                'take_profit': None,
                'reason': f"News sentiment: {interpretation} ({article_count} articles)",
                'strategy': 'news_sentiment'
            }
        elif interpretation == "Bearish":
            logger.debug(f"News sentiment: Bearish (confidence: {confidence*0.8:.2f})")
            return {
                'signal': 'SELL',
                'confidence': confidence * 0.8,
                'stop_loss': None,
                'take_profit': None,
                'reason': f"News sentiment: {interpretation} ({article_count} articles)",
                'strategy': 'news_sentiment'
            }
        else:
            logger.debug("News sentiment: Neutral - no signal")
            return None  # Neutral - no signal
    
    def weighted_vote(self, signals: Dict[str, Dict]) -> Optional[Dict]:
        """
        Combine all strategy signals using weighted voting
        Returns: Combined signal or None
        """
        if not signals:
            logger.debug("No signals to vote on")
            return None
        
        votes_for_buy = 0
        votes_for_sell = 0
        total_weight = 0
        all_reasons = []
        contributing_strategies = []
        
        logger.debug(f"Voting on {len(signals)} signals")
        
        for name, signal in signals.items():
            weight = self.strategy_weights.get(name, 1.0)
            direction = signal.get('signal', 'HOLD')
            confidence = signal.get('confidence', 0.5)
            
            # Weighted vote
            if direction == 'BUY':
                votes_for_buy += weight * confidence
                total_weight += weight
                all_reasons.append(f"{name}: BUY ({confidence:.0%})")
                contributing_strategies.append(name)
                logger.debug(f"  BUY vote: {name} (weight: {weight:.2f}, confidence: {confidence:.2f})")
            elif direction == 'SELL':
                votes_for_sell += weight * confidence
                total_weight += weight
                all_reasons.append(f"{name}: SELL ({confidence:.0%})")
                contributing_strategies.append(name)
                logger.debug(f"  SELL vote: {name} (weight: {weight:.2f}, confidence: {confidence:.2f})")
        
        if total_weight == 0:
            logger.debug("No valid votes (total weight = 0)")
            return None
        
        # Calculate vote percentages
        buy_percentage = votes_for_buy / total_weight
        sell_percentage = votes_for_sell / total_weight
        
        logger.debug(f"Vote results - BUY: {buy_percentage:.2%}, SELL: {sell_percentage:.2%}")
        
        # Determine final signal
        threshold = 0.55
        final_signal = 'HOLD'
        confidence = max(buy_percentage, sell_percentage)
        
        if buy_percentage > threshold and buy_percentage > sell_percentage:
            final_signal = 'BUY'
            logger.info(f"Voting result: BUY with {buy_percentage:.2%} confidence")
        elif sell_percentage > threshold and sell_percentage > buy_percentage:
            final_signal = 'SELL'
            logger.info(f"Voting result: SELL with {sell_percentage:.2%} confidence")
        else:
            logger.info(f"Voting result: HOLD (BUY: {buy_percentage:.2%}, SELL: {sell_percentage:.2%})")
        
        # Get the most recent price for entry
        if 'df' in self.trading_system.__dict__:
            current_price = self.trading_system.df['close'].iloc[-1]
        else:
            current_price = 0
        
        # Calculate average stop loss and take profit from voting strategies
        stop_losses = []
        take_profits = []
        for name in contributing_strategies:
            signal = signals[name]
            if 'stop_loss' in signal and signal['stop_loss'] is not None:
                stop_losses.append(signal['stop_loss'])
            if 'take_profit' in signal and signal['take_profit'] is not None:
                take_profits.append(signal['take_profit'])
        
        avg_stop_loss = np.mean(stop_losses) if stop_losses else current_price * (0.995 if final_signal == 'BUY' else 1.005)
        avg_take_profit = np.mean(take_profits) if take_profits else current_price * (1.01 if final_signal == 'BUY' else 0.99)
        
        # Record this vote for performance tracking
        vote_record = {
            'timestamp': datetime.now(),
            'signals': {k: v.get('signal') for k, v in signals.items()},  # Store only signals, not full dicts
            'buy_percentage': buy_percentage,
            'sell_percentage': sell_percentage,
            'final_signal': final_signal,
            'confidence': confidence,
            'contributing_strategies': contributing_strategies
        }
        self.vote_history.append(vote_record)
        
        # Highlight if news sentiment contributed
        news_contributed = 'news_sentiment' in contributing_strategies
        news_emoji = " 📰" if news_contributed else ""
        
        if news_contributed:
            logger.info("News sentiment contributed to this vote")
        
        return {
            'signal': final_signal,
            'confidence': confidence,
            'buy_votes': buy_percentage,
            'sell_votes': sell_percentage,
            'entry_price': current_price,
            'stop_loss': avg_stop_loss,
            'take_profit': avg_take_profit,
            'reason': f"Vote: {', '.join(all_reasons[:3])}" + (f" +{len(all_reasons)-3} more" if len(all_reasons) > 3 else ""),
            'contributing_strategies': contributing_strategies,
            'strategy_id': 'VOTING',
            'strategy_emoji': '🗳️' + news_emoji
        }
    
    def update_strategy_performance(self, trade_result: Dict):
        """
        Update performance metrics for strategies that contributed to this trade
        """
        if 'contributing_strategies' not in trade_result:
            return
        
        logger.info(f"Updating strategy performance for trade: ${trade_result.get('pnl', 0):.2f}")
        
        for strategy_name in trade_result['contributing_strategies']:
            if strategy_name in self.strategy_performance:
                perf = self.strategy_performance[strategy_name]
                perf['trades'] += 1
                
                if trade_result.get('pnl', 0) > 0:
                    perf['wins'] += 1
                    logger.debug(f"  {strategy_name}: WIN")
                else:
                    perf['losses'] += 1
                    logger.debug(f"  {strategy_name}: LOSS")
                
                perf['total_pnl'] += trade_result.get('pnl', 0)
                perf['win_rate'] = perf['wins'] / perf['trades'] if perf['trades'] > 0 else 0
                
                logger.debug(f"  {strategy_name}: now {perf['wins']}/{perf['trades']} wins ({perf['win_rate']:.1%})")
                
                # Update weight based on performance (optional)
                self._update_strategy_weight(strategy_name)
    
    def _update_strategy_weight(self, strategy_name: str):
        """
        Dynamically adjust strategy weights based on performance
        """
        perf = self.strategy_performance[strategy_name]
        if perf['trades'] < 10:
            return  # Not enough data
        
        base_weight = 1.0
        win_rate_bonus = (perf['win_rate'] - 0.5) * 2  # -1 to +1
        pnl_factor = min(max(perf['total_pnl'] / 10, -1), 1)  # Normalize
        
        new_weight = base_weight + win_rate_bonus * 0.5 + pnl_factor * 0.3
        new_weight = max(0.3, min(new_weight, 3.0))  # Keep between 0.3 and 3.0
        
        old_weight = self.strategy_weights.get(strategy_name, 1.0)
        self.strategy_weights[strategy_name] = round(new_weight, 2)
        
        logger.info(f"Strategy {strategy_name} weight updated: {old_weight:.2f} → {new_weight:.2f} (win rate: {perf['win_rate']:.1%}, P&L: ${perf['total_pnl']:.2f})")
    
    def get_performance_report(self) -> Dict:
        """
        Get performance report for all strategies including news sentiment
        """
        report = {
            'total_votes': len(self.vote_history),
            'strategies': self.strategy_performance,
            'weights': self.strategy_weights,
            'recent_votes': self.vote_history[-10:] if self.vote_history else []
        }
        
        logger.info(f"Performance report generated: {len(self.vote_history)} total votes")
        return report
    
    def get_best_strategies(self, top_n: int = 3) -> List[Tuple[str, float]]:
        """
        Get top N performing strategies by win rate
        """
        strategies = []
        for name, perf in self.strategy_performance.items():
            if perf['trades'] >= 5:  # Minimum trades threshold
                strategies.append((name, perf['win_rate']))
        
        strategies.sort(key=lambda x: x[1], reverse=True)
        
        logger.info(f"Top {min(top_n, len(strategies))} strategies: {[(s, f'{w:.1%}') for s, w in strategies[:top_n]]}")
        
        return strategies[:top_n]