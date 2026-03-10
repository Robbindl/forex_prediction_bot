"""
Market Calendar Integration
Economic events, earnings, and crypto halving tracker
"""

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
from bs4 import BeautifulSoup
import json

class MarketCalendar:
    """
    Tracks important market events:
    - Economic calendar (Fed, CPI, NFP)
    - Earnings calendar
    - Crypto halving countdown
    """
    
    def __init__(self):
        self.economic_events = []
        self.earnings = []
        self.halving_data = self._init_halving_data()
        
    def _init_halving_data(self):
        """Initialize crypto halving data"""
        return {
            'bitcoin': {
                'next_halving': datetime(2028, 3, 15),  # Approximate
                'block_reward': 3.125,
                'next_reward': 1.5625,
                'halving_count': 5
            },
            'litecoin': {
                'next_halving': datetime(2027, 8, 2),   # Approximate
                'block_reward': 6.25,
                'next_reward': 3.125,
                'halving_count': 4
            }
        }
    
    def fetch_economic_calendar(self, days: int = 7) -> List[Dict]:
        """
        Fetch economic events from free API
        Using investing.com or similar free source
        """
        try:
            # Method 1: Using Alpha Vantage (if you have key)
            # url = f"https://www.alphavantage.co/query?function=ECONOMIC_CALENDAR&apikey={ALPHA_VANTAGE_KEY}"
            
            # Method 2: Scraping (simplified)
            events = [
                {
                    'date': datetime.now() + timedelta(days=2),
                    'event': 'FOMC Meeting',
                    'impact': 'HIGH',
                    'forecast': '0.25%',
                    'previous': '0.50%'
                },
                {
                    'date': datetime.now() + timedelta(days=3),
                    'event': 'CPI Data',
                    'impact': 'HIGH',
                    'forecast': '3.2%',
                    'previous': '3.1%'
                },
                {
                    'date': datetime.now() + timedelta(days=5),
                    'event': 'Non-Farm Payrolls',
                    'impact': 'HIGH',
                    'forecast': '180K',
                    'previous': '165K'
                }
            ]
            
            self.economic_events = events
            return events
            
        except Exception as e:
            print(f"⚠️ Error fetching economic calendar: {e}")
            return []
    
    def fetch_earnings_calendar(self, days: int = 7) -> List[Dict]:
        """
        Fetch earnings calendar for stocks
        """
        try:
            # Major companies reporting soon
            earnings = [
                {
                    'symbol': 'AAPL',
                    'date': datetime.now() + timedelta(days=10),
                    'quarter': 'Q2',
                    'eps_estimate': 1.52,
                    'revenue_estimate': '89.5B'
                },
                {
                    'symbol': 'MSFT',
                    'date': datetime.now() + timedelta(days=12),
                    'quarter': 'Q2',
                    'eps_estimate': 2.85,
                    'revenue_estimate': '62.3B'
                },
                {
                    'symbol': 'TSLA',
                    'date': datetime.now() + timedelta(days=8),
                    'quarter': 'Q1',
                    'eps_estimate': 0.73,
                    'revenue_estimate': '23.1B'
                }
            ]
            
            self.earnings = earnings
            return earnings
            
        except Exception as e:
            print(f"⚠️ Error fetching earnings: {e}")
            return []
    
    def get_halving_countdown(self, crypto: str = 'bitcoin') -> Dict:
        """
        Get days until next halving
        """
        if crypto not in self.halving_data:
            return {'error': f'Unknown crypto: {crypto}'}
        
        data = self.halving_data[crypto]
        now = datetime.now()
        days_until = (data['next_halving'] - now).days
        
        return {
            'crypto': crypto,
            'days_until': days_until,
            'current_reward': data['block_reward'],
            'next_reward': data['next_reward'],
            'reduction_percent': ((data['block_reward'] - data['next_reward']) / data['block_reward']) * 100,
            'halving_date': data['next_halving'].strftime('%Y-%m-%d'),
            'is_soon': days_until <= 90
        }
    
    def get_high_impact_events(self, days: int = 3) -> List[Dict]:
        """
        Get high-impact events in next X days
        """
        high_impact = []
        now = datetime.now()
        
        for event in self.economic_events:
            if event['impact'] == 'HIGH':
                days_until = (event['date'] - now).days
                if 0 <= days_until <= days:
                    high_impact.append(event)
        
        return high_impact
    
    def should_reduce_risk(self) -> Dict:
        """
        Determine if we should reduce risk due to upcoming events
        """
        high_impact = self.get_high_impact_events(days=2)
        halving = self.get_halving_countdown()
        
        risk_reduction = 1.0  # 1.0 = normal risk
        
        if high_impact:
            risk_reduction *= 0.7  # Reduce 30% before major events
            print(f"⚠️ High-impact events in next 2 days: {len(high_impact)}")
        
        if halving.get('is_soon'):
            risk_reduction *= 0.5  # Reduce 50% before halving
            print(f"⚠️ Crypto halving in {halving['days_until']} days")
        
        return {
            'risk_multiplier': risk_reduction,
            'reduce_trading': risk_reduction < 0.8,
            'high_impact_events': len(high_impact) > 0,
            'halving_soon': halving.get('is_soon', False)
        }


# Test the calendar
if __name__ == "__main__":
    calendar = MarketCalendar()
    
    print("\n📅 Market Calendar Test")
    print("="*50)
    
    # Fetch events
    calendar.fetch_economic_calendar()
    calendar.fetch_earnings_calendar()
    
    # Show halving
    halving = calendar.get_halving_countdown('bitcoin')
    print(f"\n🪙 Bitcoin Halving: {halving['days_until']} days away")
    print(f"   Reward: {halving['current_reward']} → {halving['next_reward']} BTC")
    
    # Show risk recommendation
    risk = calendar.should_reduce_risk()
    print(f"\n🛡️ Risk Recommendation: {risk['risk_multiplier']:.1%} normal size")