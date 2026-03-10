# create_personality_tables.py
"""
Run this ONCE to create the personality tables in your database
"""

from config.database import engine, Base
from models.trade_models import TradingDiary, BotPersonality, MemorableMoments, HumanExplanations

print("🚀 Creating personality database tables...")

# Create the new tables
Base.metadata.create_all(bind=engine)

print("✅ Tables created successfully!")
print("  • trading_diary")
print("  • bot_personality")
print("  • memorable_moments")
print("  • human_explanations")

# Initialize bot personality
from services.personality_service import PersonalityDatabase
print("\n🤖 Initializing bot personality...")
db = PersonalityDatabase()
db.close()
print("✅ Bot personality initialized!")