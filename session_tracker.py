"""
Session Tracker - Track trading performance by market session
"""

from datetime import datetime, time
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from logger import logger

class SessionTracker:
    """
    Track trading performance by market session
    Asian: 00:00-09:00 EAT
    London: 09:00-17:00 EAT  
    New York: 15:00-00:00 EAT
    """
    
    SESSIONS = {
        'asia': {'name': 'Asia', 'start': 0, 'end': 9, 'emoji': '🌏'},
        'london': {'name': 'London', 'start': 9, 'end': 17, 'emoji': '🇬🇧'},
        'ny': {'name': 'New York', 'start': 15, 'end': 24, 'emoji': '🇺🇸'},
        'overlap': {'name': 'London-NY Overlap', 'start': 15, 'end': 17, 'emoji': '🤝'}
    }
    
    def __init__(self):
        self.session_stats = {
            'asia': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'london': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'ny': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'overlap': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}
        }
        self.trade_history = []  # Store all trades with session info
    
    def get_current_session(self) -> Dict:
        """Get current trading session based on EAT time"""
        now = datetime.now()
        current_hour = now.hour
        
        for session_id, session_info in self.SESSIONS.items():
            start = session_info['start']
            end = session_info['end']
            
            # Handle sessions that cross midnight
            if end > start:
                if start <= current_hour < end:
                    return {
                        'session_id': session_id,
                        'name': session_info['name'],
                        'emoji': session_info['emoji'],
                        'current_hour': current_hour
                    }
            else:  # Session crosses midnight (e.g., 22:00 - 04:00)
                if current_hour >= start or current_hour < end:
                    return {
                        'session_id': session_id,
                        'name': session_info['name'],
                        'emoji': session_info['emoji'],
                        'current_hour': current_hour
                    }
        
        # Default to Asia if no session found (shouldn't happen)
        return {
            'session_id': 'asia',
            'name': 'Asia',
            'emoji': '🌏',
            'current_hour': current_hour
        }
    
    def get_session_for_time(self, dt: datetime) -> str:
        """Get session ID for a specific datetime"""
        hour = dt.hour
        
        for session_id, session_info in self.SESSIONS.items():
            start = session_info['start']
            end = session_info['end']
            
            if end > start:
                if start <= hour < end:
                    return session_id
            else:
                if hour >= start or hour < end:
                    return session_id
        
        return 'asia'  # Default
    
    def record_trade(self, trade: Dict) -> None:
        """
        Record a trade and update session statistics
        
        Args:
            trade: Trade dictionary with 'entry_time', 'pnl', etc.
        """
        # Get entry time
        if 'entry_time' in trade:
            if isinstance(trade['entry_time'], str):
                entry_time = datetime.fromisoformat(trade['entry_time'])
            else:
                entry_time = trade['entry_time']
        else:
            entry_time = datetime.now()
        
        # Determine session
        session_id = self.get_session_for_time(entry_time)
        
        # Add session info to trade
        trade_with_session = trade.copy()
        trade_with_session['session'] = session_id
        trade_with_session['session_name'] = self.SESSIONS[session_id]['name']
        self.trade_history.append(trade_with_session)
        
        # Update statistics
        stats = self.session_stats[session_id]
        stats['trades'] += 1
        
        if trade.get('pnl', 0) > 0:
            stats['wins'] += 1
        else:
            stats['losses'] += 1
        
        stats['pnl'] += trade.get('pnl', 0)
    
    def get_session_performance(self, session_id: Optional[str] = None) -> Dict:
        """
        Get performance statistics for a specific session or all sessions
        
        Args:
            session_id: Optional session ID (asia, london, ny, overlap)
        
        Returns:
            Performance statistics with win rate, avg P&L, etc.
        """
        if session_id:
            stats = self.session_stats[session_id]
            trades = stats['trades']
            
            if trades > 0:
                win_rate = (stats['wins'] / trades) * 100
                avg_pnl = stats['pnl'] / trades
            else:
                win_rate = 0
                avg_pnl = 0
            
            return {
                'session': self.SESSIONS[session_id]['name'],
                'emoji': self.SESSIONS[session_id]['emoji'],
                'trades': stats['trades'],
                'wins': stats['wins'],
                'losses': stats['losses'],
                'win_rate': round(win_rate, 2),
                'total_pnl': round(stats['pnl'], 2),
                'avg_pnl': round(avg_pnl, 2)
            }
        else:
            # Return all sessions
            results = {}
            for sid in self.SESSIONS.keys():
                results[sid] = self.get_session_performance(sid)
            return results
    
    def get_best_session(self) -> Dict:
        """Get the best performing session by win rate"""
        best_session = None
        best_win_rate = -1
        
        for session_id in self.SESSIONS.keys():
            perf = self.get_session_performance(session_id)
            if perf['trades'] >= 5:  # Minimum sample size
                if perf['win_rate'] > best_win_rate:
                    best_win_rate = perf['win_rate']
                    best_session = perf
        
        if best_session:
            return best_session
        else:
            return {'message': 'Not enough data to determine best session'}
    
    def get_worst_session(self) -> Dict:
        """Get the worst performing session by win rate"""
        worst_session = None
        worst_win_rate = 101
        
        for session_id in self.SESSIONS.keys():
            perf = self.get_session_performance(session_id)
            if perf['trades'] >= 5:  # Minimum sample size
                if perf['win_rate'] < worst_win_rate:
                    worst_win_rate = perf['win_rate']
                    worst_session = perf
        
        if worst_session:
            return worst_session
        else:
            return {'message': 'Not enough data to determine worst session'}
    
    def get_recommended_session(self) -> Dict:
        """
        Get recommended trading session based on historical performance
        """
        best = self.get_best_session()
        if 'message' in best:
            return {'recommendation': 'Trade any session', 'reason': 'Insufficient data'}
        
        worst = self.get_worst_session()
        
        return {
            'recommendation': f"Focus on {best['session']} session",
            'best_session': best,
            'worst_session': worst,
            'reason': f"{best['win_rate']}% win rate vs {worst['win_rate']}% in {worst['session']}"
        }
    
    def get_session_trades(self, session_id: str, limit: int = 20) -> List[Dict]:
        """Get recent trades from a specific session"""
        session_trades = [
            t for t in self.trade_history 
            if t.get('session') == session_id
        ]
        return sorted(session_trades, key=lambda x: x.get('entry_time', ''), reverse=True)[:limit]
    
    def analyze_by_hour(self) -> pd.DataFrame:
        """Analyze performance by hour of day"""
        hourly_stats = {}
        
        for hour in range(24):
            hourly_stats[hour] = {
                'trades': 0,
                'wins': 0,
                'pnl': 0.0
            }
        
        for trade in self.trade_history:
            if 'entry_time' in trade:
                if isinstance(trade['entry_time'], str):
                    dt = datetime.fromisoformat(trade['entry_time'])
                else:
                    dt = trade['entry_time']
                
                hour = dt.hour
                hourly_stats[hour]['trades'] += 1
                if trade.get('pnl', 0) > 0:
                    hourly_stats[hour]['wins'] += 1
                hourly_stats[hour]['pnl'] += trade.get('pnl', 0)
        
        # Convert to DataFrame
        data = []
        for hour, stats in hourly_stats.items():
            if stats['trades'] > 0:
                win_rate = (stats['wins'] / stats['trades']) * 100
            else:
                win_rate = 0
            
            data.append({
                'hour': hour,
                'trades': stats['trades'],
                'wins': stats['wins'],
                'win_rate': round(win_rate, 2),
                'pnl': round(stats['pnl'], 2)
            })
        
        df = pd.DataFrame(data)
        return df.sort_values('win_rate', ascending=False)
    
    def get_summary_report(self) -> Dict:
        """Get comprehensive session performance report"""
        all_sessions = self.get_session_performance()
        best = self.get_best_session()
        worst = self.get_worst_session()
        hourly_df = self.analyze_by_hour()
        
        # Get top 3 hours
        top_hours = hourly_df.head(3)[['hour', 'win_rate', 'pnl']].to_dict('records')
        
        return {
            'total_trades': len(self.trade_history),
            'sessions': all_sessions,
            'best_session': best,
            'worst_session': worst,
            'top_hours': top_hours,
            'recommendation': self.get_recommended_session()
        }
    
    def reset_stats(self):
        """Reset all statistics"""
        self.session_stats = {
            'asia': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'london': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'ny': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0},
            'overlap': {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}
        }
        self.trade_history = []
        logger.info("✅ Session statistics reset")

# Example usage
if __name__ == "__main__":
    # Test the session tracker
    tracker = SessionTracker()
    
    # Get current session
    current = tracker.get_current_session()
    logger.info(f"Current Session: {current['emoji']} {current['name']}")

    # Record some test trades
    test_trades = [
        {'entry_time': datetime.now(), 'pnl': 50, 'asset': 'BTC-USD'},
        {'entry_time': datetime.now(), 'pnl': -20, 'asset': 'ETH-USD'},
        {'entry_time': datetime.now(), 'pnl': 30, 'asset': 'EUR/USD'},
    ]
    
    for trade in test_trades:
        tracker.record_trade(trade)
    
    # Get performance report
    report = tracker.get_summary_report()
    logger.info("\n📊 Session Performance:")

    for session, stats in report['sessions'].items():
        if isinstance(stats, dict) and 'trades' in stats:
            logger.info(f"  {stats['emoji']} {stats['session']}: {stats['win_rate']}% win rate ({stats['trades']} trades)")

    if 'best_session' in report and 'message' not in report['best_session']:
        logger.info(f"\n🏆 Best Session: {report['best_session']['emoji']} {report['best_session']['session']}")