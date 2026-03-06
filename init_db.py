#!/usr/bin/env python
"""
Run this ONCE to create all database tables
"""

import sys
from pathlib import Path

# Add project folder to Python path
sys.path.append(str(Path(__file__).parent))

from config.database import engine, Base
from models.trade_models import Trade

print("🚀 Creating database tables...")

# This creates all tables defined in your models
Base.metadata.create_all(bind=engine)

print("✅ Database tables created successfully!")
print("\nTables created:")
print("  • trades")