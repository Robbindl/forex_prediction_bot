#!/usr/bin/env python
"""
Run this ONCE to create all database tables
"""

import sys
from pathlib import Path

# Add project folder to Python path
sys.path.append(str(Path(__file__).parent))

from config.database import engine, Base, SessionLocal
from models.trade_models import Trade

print("🚀 Creating database tables...")

if not engine:
    print("❌ Database engine not available")
    print("\nPlease check:")
    print("1. PostgreSQL is running")
    print("2. .env file has DATABASE_URL configured")
    print("3. Database exists and credentials are correct")
    sys.exit(1)

try:
    # This creates all tables defined in your models
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables created successfully!")
    
    # Test connection
    db = SessionLocal()
    db.execute("SELECT 1")
    db.close()
    print("✅ Database connection verified")
    
    print("\nTables created:")
    print("  • trades")
    
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)