"""
📊 View Current Trades
"""

import json
from datetime import datetime

with open('paper_trades.json', 'r') as f:
    data = json.load(f)

print("\n" + "="*60)
print("📊 CURRENT OPEN POSITIONS")
print("="*60)

for trade in data['open_positions']:
    print(f"\n🪙 {trade['asset']} ({trade['category']})")
    print(f"   Signal: {trade['signal']}")
    print(f"   Entry: ${trade['entry_price']:.2f}")
    print(f"   Size: {trade['position_size']:.6f}")
    print(f"   Stop Loss: ${trade['stop_loss']:.2f}")
    print(f"   Confidence: {trade['confidence']*100:.0f}%")
    print(f"   Reason: {trade['reason']}")
    print(f"   Entered: {trade['entry_time']}")

print("\n" + "="*60)
print(f"Total Open Positions: {len(data['open_positions'])}")
print("="*60)