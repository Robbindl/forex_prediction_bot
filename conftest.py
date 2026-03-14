"""
conftest.py — Mocks database for all tests. No PostgreSQL required to run tests.
Must be in project root next to bot.py.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

# ── 1. Add project root to path ───────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

# ── 2. Mock database BEFORE any module imports config.database ────────────────
_fake_engine       = MagicMock()
_fake_session      = MagicMock()
_fake_session.__enter__ = MagicMock(return_value=_fake_session)
_fake_session.__exit__  = MagicMock(return_value=False)
_fake_session_local     = MagicMock(return_value=_fake_session)

_fake_db_module              = MagicMock()
_fake_db_module.engine       = _fake_engine
_fake_db_module.SessionLocal = _fake_session_local
_fake_db_module.Base         = MagicMock()
_fake_db_module.init_db      = MagicMock()
_fake_db_module.get_db       = MagicMock()
sys.modules["config.database"] = _fake_db_module

# ── 3. Mock db_pool so SystemState never calls real DB ────────────────────────
_fake_db_service = MagicMock()
_fake_db_service.use_db                  = False
_fake_db_service.save_open_position      = MagicMock()
_fake_db_service.delete_open_position    = MagicMock()
_fake_db_service.load_open_positions     = MagicMock(return_value=[])
_fake_db_service.save_trade              = MagicMock(return_value="test_id")
_fake_db_service.upsert_daily_stats      = MagicMock()
_fake_db_service.get_recent_trades       = MagicMock(return_value=[])
_fake_db_service.get_performance_summary = MagicMock(return_value={})
_fake_db_service.ping                    = MagicMock(return_value=True)

_fake_pool_module         = MagicMock()
_fake_pool_module.get_db  = MagicMock(return_value=_fake_db_service)
sys.modules["services.db_pool"]           = _fake_pool_module
sys.modules["services.database_service"]  = MagicMock()

# ── 4. Mock models so SQLAlchemy never tries to connect ───────────────────────
sys.modules["models.trade_models"] = MagicMock()