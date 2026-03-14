"""
tests/test_services.py — Database and Gateway connectivity tests.

  Unit tests  — always run, no external services required.
  Integration — skipped automatically when the service isn't reachable.

Run just unit tests:
    pytest tests/test_services.py -v -m "not integration"

Run everything (requires live services):
    pytest tests/test_services.py -v
"""
from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT         = Path(__file__).parent.parent
GATEWAY_PORT = 8081
DB_PORT      = 5432
GATEWAY_DIR  = ROOT / "gateway"

sys.path.insert(0, str(ROOT))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _port_open(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _node_available() -> bool:
    return bool(shutil.which("node") or shutil.which("node.exe"))


def _npm_available() -> bool:
    return bool(shutil.which("npm") or shutil.which("npm.cmd"))


def _our_gateway_running() -> bool:
    """
    Returns True only if OUR Node.js gateway is on port 8081 —
    verified by checking /health for {"gateway": "ok"}.
    """
    if not _port_open(GATEWAY_PORT):
        return False
    import urllib.request, json as _json
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{GATEWAY_PORT}/health", timeout=2
        ) as r:
            data = _json.loads(r.read())
        return data.get("gateway") == "ok"
    except Exception:
        return False


# ── Unit: port probe ──────────────────────────────────────────────────────────

class TestPortProbe:

    def test_closed_port_returns_false(self):
        assert _port_open(19999) is False

    def test_open_port_returns_true(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.listen(1)
        try:
            assert _port_open(port) is True
        finally:
            s.close()

    def test_timeout_is_respected(self):
        start = time.time()
        _port_open(19998, host="10.255.255.1", timeout=0.3)
        assert time.time() - start < 2.0

    def test_returns_bool_not_socket(self):
        assert isinstance(_port_open(19999), bool)

    def test_invalid_host_returns_false(self):
        assert _port_open(80, host="invalid.invalid.") is False


# ── Unit: gateway file structure ──────────────────────────────────────────────

class TestGatewayFiles:

    def test_gateway_directory_exists(self):
        assert GATEWAY_DIR.exists(), "gateway/ directory missing from project root"

    def test_server_js_exists(self):
        assert (GATEWAY_DIR / "server.js").exists(), "gateway/server.js is missing"

    def test_package_json_exists(self):
        assert (GATEWAY_DIR / "package.json").exists(), "gateway/package.json is missing"

    def test_package_json_is_valid(self):
        pkg = json.loads((GATEWAY_DIR / "package.json").read_text(encoding="utf-8"))
        assert "name" in pkg
        assert "dependencies" in pkg

    def test_package_json_has_required_deps(self):
        deps = json.loads(
            (GATEWAY_DIR / "package.json").read_text(encoding="utf-8")
        )["dependencies"]
        for required in ("ws", "express", "cors"):
            assert required in deps, f"gateway/package.json missing dependency: {required}"

    def test_server_js_declares_ws_port(self):
        src = (GATEWAY_DIR / "server.js").read_text(encoding="utf-8")
        assert "8081" in src, "gateway/server.js does not reference port 8081"

    def test_server_js_has_health_endpoint(self):
        src = (GATEWAY_DIR / "server.js").read_text(encoding="utf-8")
        assert "/health" in src, "gateway/server.js missing /health endpoint"

    def test_server_js_handles_redis_failure(self):
        src = (GATEWAY_DIR / "server.js").read_text(encoding="utf-8")
        assert "lazyConnect" in src or "retryStrategy" in src, (
            "server.js has no Redis retry/lazy strategy — will crash if Redis is absent"
        )


# ── Unit: Node.js availability ────────────────────────────────────────────────

class TestNodeAvailability:

    def test_node_is_detectable(self):
        if not _node_available():
            pytest.skip("Node.js not installed")
        assert shutil.which("node") or shutil.which("node.exe")

    def test_node_is_executable(self):
        if not _node_available():
            pytest.skip("Node.js not installed")
        node = shutil.which("node") or shutil.which("node.exe")
        result = subprocess.run(
            [node, "--version"], capture_output=True, text=True, timeout=5
        )
        assert result.returncode == 0
        assert result.stdout.startswith("v")

    def test_npm_is_detectable(self):
        if not _npm_available():
            pytest.skip("npm not installed")
        assert shutil.which("npm") or shutil.which("npm.cmd")

    def test_node_version_is_14_or_higher(self):
        if not _node_available():
            pytest.skip("Node.js not installed")
        node = shutil.which("node") or shutil.which("node.exe")
        result = subprocess.run(
            [node, "--version"], capture_output=True, text=True, timeout=5
        )
        major = int(result.stdout.strip().lstrip("v").split(".")[0])
        assert major >= 14, f"Node.js v{major} is too old — gateway requires v14+"


# ── Unit: bot.py gateway logic ────────────────────────────────────────────────

class TestBotGatewayLogic:

    def test_start_gateway_skips_when_node_missing(self):
        with patch("shutil.which", return_value=None):
            import importlib, bot
            importlib.reload(bot)
            assert bot.start_gateway() is None

    def test_start_gateway_skips_when_already_listening(self):
        with patch("bot._port_open", return_value=True):
            import bot
            assert bot.start_gateway(force=False) is None

    def test_gateway_is_running_matches_port_probe(self):
        import bot
        assert bot.gateway_is_running() == _port_open(GATEWAY_PORT)

    def test_stop_gateway_is_safe_when_never_started(self):
        import bot
        bot._gateway_proc = None
        bot.stop_gateway()  # must not raise

    def test_bot_has_no_gateway_flag(self):
        src = open(ROOT / "bot.py", encoding="utf-8").read()
        assert "--no-gateway" in src

    def test_gateway_killed_on_stop_gateway(self):
        import bot
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        bot._gateway_proc = mock_proc
        bot.stop_gateway()
        mock_proc.terminate.assert_called_once()


# ── Unit: database config ─────────────────────────────────────────────────────

class TestDatabaseConfig:

    def test_database_url_is_set(self):
        from config.config import DATABASE_URL
        assert DATABASE_URL

    def test_database_url_starts_with_postgresql(self):
        from config.config import DATABASE_URL
        assert DATABASE_URL.startswith("postgresql")

    def test_database_url_has_host_and_dbname(self):
        from config.config import DATABASE_URL
        assert "@" in DATABASE_URL
        assert "/" in DATABASE_URL.split("@")[-1]

    def test_db_port_is_integer(self):
        from config.config import DB_PORT
        assert isinstance(DB_PORT, int)
        assert 1 <= DB_PORT <= 65535

    def test_db_host_is_set(self):
        from config.config import DB_HOST
        assert DB_HOST

    def test_db_name_is_set(self):
        from config.config import DB_NAME
        assert DB_NAME


# ── Unit: Flask /api/gateway/status ──────────────────────────────────────────

class TestGatewayStatusEndpoint:

    @pytest.fixture
    def client(self):
        from dashboard.web_app_live import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c

    def test_endpoint_returns_200(self, client):
        assert client.get("/api/gateway/status").status_code == 200

    def test_endpoint_returns_json(self, client):
        assert client.get("/api/gateway/status").get_json() is not None

    def test_response_has_running_field(self, client):
        assert "running" in client.get("/api/gateway/status").get_json()

    def test_response_has_port_field(self, client):
        assert client.get("/api/gateway/status").get_json().get("port") == 8081

    def test_response_has_url_field(self, client):
        data = client.get("/api/gateway/status").get_json()
        assert "url" in data
        assert data["url"].startswith("ws://")

    def test_running_is_bool(self, client):
        assert isinstance(
            client.get("/api/gateway/status").get_json()["running"], bool
        )

    def test_running_matches_real_port_state(self, client):
        data   = client.get("/api/gateway/status").get_json()
        actual = _port_open(GATEWAY_PORT)
        assert data["running"] == actual


# ── Unit: accuracy_dashboard.html JavaScript ─────────────────────────────────

class TestAccuracyDashboardJs:

    def _src(self) -> str:
        return (
            ROOT / "templates" / "accuracy_dashboard.html"
        ).read_text(encoding="utf-8")

    def test_checks_status_before_connecting(self):
        assert "/api/gateway/status" in self._src()

    def test_guards_against_gateway_down(self):
        assert "if (!d.running) return" in self._src()

    def test_no_unconditional_websocket_connect(self):
        src        = self._src()
        status_pos = src.find("/api/gateway/status")
        ws_pos     = src.find("new WebSocket(")
        assert status_pos != -1 and ws_pos != -1
        assert status_pos < ws_pos, (
            "WebSocket connect appears before /api/gateway/status check"
        )

    def test_uses_url_from_status_response(self):
        src = self._src()
        assert "d.url" in src or "d.running" in src

    def test_no_hardcoded_8080(self):
        assert "8080" not in self._src(), (
            "accuracy_dashboard.html still references old port 8080"
        )


# ── Integration: live database ────────────────────────────────────────────────

@pytest.mark.integration
class TestDatabaseLive:
    """
    Requires a running PostgreSQL instance.
    Automatically skipped if DB port is not reachable.
    """

    @pytest.fixture(autouse=True)
    def require_db(self):
        from config.config import DB_HOST, DB_PORT
        if not _port_open(DB_PORT, host=DB_HOST):
            pytest.skip(f"PostgreSQL not reachable at {DB_HOST}:{DB_PORT}")

    def test_db_port_is_open(self):
        from config.config import DB_HOST, DB_PORT
        assert _port_open(DB_PORT, host=DB_HOST)

    def test_sqlalchemy_can_connect(self):
        from sqlalchemy import create_engine, text
        from config.config import DATABASE_URL
        try:
            engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            with engine.connect() as conn:
                assert conn.execute(text("SELECT 1")).scalar() == 1
        except Exception as e:
            pytest.fail(f"SQLAlchemy connection failed: {e}")

    def test_all_tables_exist(self):
        """
        Bypasses conftest mocks for both config.database AND models.trade_models
        so the real SQLAlchemy models register themselves to Base before
        init_db() calls Base.metadata.create_all().
        """
        import sys
        import importlib
        from sqlalchemy import create_engine, inspect
        from config.config import DATABASE_URL

        # Save and remove all mocks that block real table creation
        saved = {}
        for mod in ("config.database", "models.trade_models"):
            saved[mod] = sys.modules.pop(mod, None)

        try:
            # Import models first so they register to Base
            importlib.import_module("models.trade_models")
            # Now import and run the real init_db
            real_db = importlib.import_module("config.database")
            real_db.init_db()
        except Exception as e:
            pytest.fail(f"init_db() failed: {e}")
        finally:
            # Restore all mocks so other tests are unaffected
            for mod, mock in saved.items():
                if mock is not None:
                    sys.modules[mod] = mock

        inspector = inspect(create_engine(DATABASE_URL))
        tables    = inspector.get_table_names()
        missing   = [t for t in ("trades", "open_positions", "daily_stats")
                    if t not in tables]
        assert not missing, (
            f"Tables missing after init_db(): {missing}\n"
            f"Existing: {tables}"
        )

    def test_can_write_and_read(self):
        from sqlalchemy import create_engine, text
        from config.config import DATABASE_URL
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS _test_ping "
                "(id SERIAL PRIMARY KEY, val TEXT)"
            ))
            conn.execute(text("INSERT INTO _test_ping (val) VALUES ('ok')"))
            row = conn.execute(
                text("SELECT val FROM _test_ping LIMIT 1")
            ).fetchone()
            assert row[0] == "ok"
            conn.execute(text("DROP TABLE _test_ping"))
            conn.commit()


# ── Integration: live gateway ─────────────────────────────────────────────────

@pytest.mark.integration
class TestGatewayLive:
    """
    Requires the Node.js gateway running on port 8081.
    Port 8081 is used instead of 8080 because EnterpriseDB
    (PostgreSQL web panel) occupies 8080 on Windows.

    Start gateway:  python bot.py  (auto-starts it)
    Or manually:    cd gateway && node server.js
    """

    @pytest.fixture(autouse=True)
    def require_gateway(self):
        if not _our_gateway_running():
            pytest.skip(
                f"Our Node.js gateway not running on port {GATEWAY_PORT}. "
                "Run: python bot.py  OR  cd gateway && node server.js"
            )

    def test_port_is_open(self):
        assert _port_open(GATEWAY_PORT)

    def test_health_endpoint_responds(self):
        import urllib.request, json as _json
        with urllib.request.urlopen(
            f"http://127.0.0.1:{GATEWAY_PORT}/health", timeout=3
        ) as r:
            data = _json.loads(r.read())
        assert data.get("gateway") == "ok"

    def test_health_has_clients_field(self):
        import urllib.request, json as _json
        with urllib.request.urlopen(
            f"http://127.0.0.1:{GATEWAY_PORT}/health", timeout=3
        ) as r:
            data = _json.loads(r.read())
        assert "clients" in data

    def test_health_has_uptime_field(self):
        import urllib.request, json as _json
        with urllib.request.urlopen(
            f"http://127.0.0.1:{GATEWAY_PORT}/health", timeout=3
        ) as r:
            data = _json.loads(r.read())
        assert "uptime" in data
        assert data["uptime"] >= 0

    def test_websocket_accepts_connection(self):
        try:
            import websocket as _ws
        except ImportError:
            pytest.skip("websocket-client not installed: pip install websocket-client")

        msgs = []
        def on_msg(ws, msg): msgs.append(msg); ws.close()

        ws = _ws.WebSocketApp(
            f"ws://127.0.0.1:{GATEWAY_PORT}",
            on_message=on_msg,
            on_error=lambda ws, e: None,
        )
        import threading
        t = threading.Thread(target=lambda: ws.run_forever(ping_timeout=3))
        t.daemon = True
        t.start()
        t.join(timeout=5)

        assert len(msgs) >= 1, "No message received from gateway"
        env = json.loads(msgs[0])
        assert env.get("channel") == "system"
        assert env.get("data", {}).get("type") == "welcome"

    def test_flask_gateway_status_reports_running(self):
        from dashboard.web_app_live import app
        app.config["TESTING"] = True
        with app.test_client() as c:
            data = c.get("/api/gateway/status").get_json()
        assert data["running"] is True    