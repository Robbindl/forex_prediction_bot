"""
Database configuration for trading bot
This file handles connecting to PostgreSQL
"""

import os
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import time
from logger import logger

# Load your .env file
load_dotenv()

# Get database URL from .env
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/trading_bot')

def create_db_engine(max_retries=3):
    """Create database engine with retry logic"""
    for attempt in range(max_retries):
        try:
            engine = create_engine(
                DATABASE_URL,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False
            )
            # Test connection - FIX THIS LINE
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))  # Add 'text' import
            logger.info("✅ Database connected successfully")

            return engine
        except Exception as e:
            if attempt == max_retries - 1:
                logger.info(f"❌ Database connection failed after {max_retries} attempts: {e}")

                return None
            logger.info(f"⚠️ Database connection attempt {attempt + 1} failed, retrying...")

            time.sleep(2)

# Create engine with retry
engine = create_db_engine()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None

# Base class for all database tables
Base = declarative_base()

def get_db():
    """Get a database session"""
    if not SessionLocal:
        logger.info("⚠️ Database not available, using file storage only")

        return None
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Create all tables if they don't exist"""
    if engine:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created/verified")

    else:
        logger.info("⚠️ Cannot create tables - database not available")