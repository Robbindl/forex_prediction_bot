"""Database connection pooling with retry logic"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import OperationalError
import time
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabasePool:
    def __init__(self, database_url, pool_size=10, max_retries=3):
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_retries = max_retries
        self._engine = None
        self._session_factory = None
        
    def get_engine(self):
        """Get or create engine with retry"""
        if self._engine is None:
            self._engine = self._create_engine_with_retry()
        return self._engine
    
    def _create_engine_with_retry(self):
        """Create engine with exponential backoff retry"""
        for attempt in range(self.max_retries):
            try:
                engine = create_engine(
                    self.database_url,
                    pool_size=self.pool_size,
                    max_overflow=20,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    echo=False
                )
                # Test connection
                with engine.connect() as conn:
                    conn.execute("SELECT 1")
                logger.info(f"✅ Database connected (attempt {attempt+1})")
                return engine
            except OperationalError as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"❌ Database connection failed after {self.max_retries} attempts")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"⚠️ DB connection failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    def get_session(self):
        """Get a new database session"""
        engine = self.get_engine()
        if self._session_factory is None:
            self._session_factory = scoped_session(
                sessionmaker(bind=engine)
            )
        return self._session_factory()
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations"""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()

# Create global instance
db_pool = None

def init_db_pool(database_url):
    global db_pool
    db_pool = DatabasePool(database_url)
    return db_pool

def get_db():
    """Get database session from pool"""
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")
    return db_pool.session_scope()