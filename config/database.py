"""
Database configuration for trading bot
This file handles connecting to PostgreSQL
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load your .env file (where we put the password)
load_dotenv()

# Get database URL from .env - this will connect to your PostgreSQL
# The format is: postgresql://username:password@localhost:port/database_name
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:your_password@localhost:5432/trading_bot')

# Create the database engine (this is what actually connects)
engine = create_engine(
    DATABASE_URL,
    pool_size=5,              # Keep 5 connections ready
    max_overflow=10,           # Allow up to 10 extra if needed
    pool_pre_ping=True,        # Test connections before using them
    echo=False                 # Set to True if you want to see SQL commands
)

# Create a session factory (sessions are what you use to talk to the DB)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for all your database tables
Base = declarative_base()

def get_db():
    """Get a database session - use this whenever you need to talk to the DB"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Create all tables if they don't exist - run this once at startup"""
    Base.metadata.create_all(bind=engine)
    print("✅ Database tables created/verified")