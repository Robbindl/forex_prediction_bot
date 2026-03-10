#!/usr/bin/env python
"""
🚀 COMPLETE DATABASE SETUP - ONE FILE TO RULE THEM ALL!
Creates tables, migrates trades, initializes personality - ALL IN ONE!
"""

import json
import os
import sys
from datetime import datetime
import uuid

print("="*60)
print("🚀 COMPLETE DATABASE SETUP")
print("="*60)

# ===== STEP 1: Create Tables =====
print("\n📦 STEP 1: Creating Database Tables")
print("-" * 40)

from config.database import engine, Base
from models.trade_models import TradingDiary, BotPersonality, MemorableMoments, HumanExplanations, Trade

# Create all tables
Base.metadata.create_all(bind=engine)
print("✅ All tables created successfully!")
print("  • trades")
print("  • trading_diary")
print("  • bot_personality")
print("  • memorable_moments")
print("  • human_explanations")

# ===== STEP 2: Migrate Trades from paper_trades.json =====
print("\n📊 STEP 2: Migrating Trades from paper_trades.json")
print("-" * 40)

from services.database_service import DatabaseService

# Load JSON file
try:
    with open('paper_trades.json', 'r') as f:
        data = json.load(f)
    print("✅ Loaded paper_trades.json")
except FileNotFoundError:
    print("⚠️ No paper_trades.json found - skipping trades migration")
    data = {'closed_positions': [], 'open_positions': []}

# Connect to database
db = DatabaseService()

# Migrate closed positions
closed_count = 0
for trade in data.get('closed_positions', []):
    # Ensure trade has trade_id
    if 'trade_id' not in trade:
        trade['trade_id'] = str(uuid.uuid4())[:8]
    db.save_trade(trade)
    closed_count += 1
    if closed_count % 10 == 0:
        print(f"  ➜ Migrated {closed_count} closed trades...")

# Migrate open positions
open_count = 0
for trade in data.get('open_positions', []):
    trade_data = trade.copy()
    trade_data['exit_time'] = None  # Mark as open
    
    if 'trade_id' not in trade_data:
        trade_data['trade_id'] = str(uuid.uuid4())[:8]
    
    db.save_trade(trade_data)
    open_count += 1
    if open_count % 10 == 0:
        print(f"  ➜ Migrated {open_count} open trades...")

db.close()

print(f"\n✅ Trade migration complete!")
print(f"  • {closed_count} closed trades imported")
print(f"  • {open_count} open trades imported")

# ===== STEP 3: Initialize Bot Personality =====
print("\n🤖 STEP 3: Initializing Bot Personality")
print("-" * 40)

from services.personality_service import PersonalityDatabase

try:
    personality_db = PersonalityDatabase()  # This auto-creates default personality
    print("✅ Bot personality initialized in database")
    
    # Check what was created
    from sqlalchemy import text
    result = personality_db.session.execute(text("SELECT * FROM bot_personality")).first()
    if result:
        print(f"  • Bot name: {result[13]}")  # bot_name column
        print(f"  • Initial mood: {result[5]} {result[6]}")  # current_mood, mood_emoji
    
    personality_db.close()
    
except Exception as e:
    print(f"⚠️ Could not initialize personality: {e}")

# ===== STEP 4: Check for Existing Diary Data =====
print("\n📔 STEP 4: Checking for Existing Diary Data")
print("-" * 40)

if os.path.exists('trading_diary.json'):
    try:
        with open('trading_diary.json', 'r') as f:
            diary_data = json.load(f)
        entries = len(diary_data.get('entries', []))
        print(f"📦 Found trading_diary.json with {entries} entries")
        print("  ⚠️ Diary entries reference trades - import after trades are migrated")
        print("  💡 To import diary entries, run this after:")
        print("     python -c \"from services.personality_service import PersonalityDatabase; db=PersonalityDatabase(); [db.record_trade_in_diary(e) for e in json.load(open('trading_diary.json'))['entries']]; db.close()\"")
    except Exception as e:
        print(f"⚠️ Error reading trading_diary.json: {e}")
else:
    print("✅ No existing trading diary found - starting fresh")

# ===== STEP 5: Verify Everything =====
print("\n🔍 STEP 5: Verifying Database")
print("-" * 40)

db = DatabaseService()
try:
    from sqlalchemy import text
    
    # Check trades
    trade_count = db.session.execute(text("SELECT COUNT(*) FROM trades")).scalar()
    print(f"✅ Trades: {trade_count} records")
    
    # Check new tables
    tables = [
        ('trading_diary', '📔'),
        ('bot_personality', '🤖'),
        ('memorable_moments', '🏆'),
        ('human_explanations', '💬')
    ]
    
    for table, emoji in tables:
        try:
            count = db.session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"{emoji} {table}: {count} records")
        except Exception as e:
            print(f"⚠️ {table}: Error - {str(e)[:50]}")
            
except Exception as e:
    print(f"⚠️ Verification error: {e}")

db.close()

# ===== STEP 6: Summary & Next Steps =====
print("\n" + "="*60)
print("🎉 DATABASE SETUP COMPLETE!")
print("="*60)
print("\n📊 Summary:")
print(f"  • Trades imported: {closed_count + open_count}")
print(f"  • Personality: {'✅ Ready' if personality_db else '⚠️ Check logs'}")
print(f"  • All tables: ✅ Created")

print("\n🚀 Next Steps:")
print("  1. Start your bot: python trading_system.py --mode live --balance 30")
print("  2. Test Telegram commands:")
print("     • /why BTC     - Get human explanation")
print("     • /why ETH     - Get explanation for Ethereum")
print("     • /why GOLD    - Get explanation for Gold")
print("     • /mood        - Check my current mood")
print("     • /diary       - See my trading diary")
print("  3. Check database manually:")
print("     docker exec -it trading-bot-db psql -U postgres -d trading_bot")
print("\n" + "="*60)