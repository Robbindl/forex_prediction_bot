#!/usr/bin/env python
"""
Import existing trades from paper_trades.json into PostgreSQL
"""

import json
from services.database_service import DatabaseService

print("🔄 Migrating JSON trades to database...")

# Load JSON file
try:
    with open('paper_trades.json', 'r') as f:
        data = json.load(f)
except FileNotFoundError:
    print("❌ No paper_trades.json found")
    exit()

# Connect to database
db = DatabaseService()

# Migrate closed positions
closed_count = 0
for trade in data.get('closed_positions', []):
    db.save_trade(trade)
    closed_count += 1
    if closed_count % 10 == 0:
        print(f"  Migrated {closed_count} closed trades...")

# Migrate open positions
open_count = 0
for trade in data.get('open_positions', []):
    trade_data = trade.copy()
    trade_data['exit_time'] = None  # Mark as open
    db.save_trade(trade_data)
    open_count += 1

db.close()

print(f"\n✅ Migration complete!")
print(f"  • {closed_count} closed trades imported")
print(f"  • {open_count} open trades imported")