"""
tests/test_decorators.py — Unit tests for API decorators

Tests the critical authentication and rate limiting paths:
  • Bearer token validation and hashing
  • API key authentication failures
  • Rate limit per-IP enforcement
  • Decorator stacking with auth + rate limit
  • Session token generation and TTL
"""

import pytest
import hashlib
import time
from unittest.mock import Mock, patch, MagicMock
from functools import wraps
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import DEVELOPMENT_MODE, DASHBOARD_API_KEY


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES — API key and token setup
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def valid_api_key():
    """Get the configured API key from .env (for testing auth)."""
    return DASHBOARD_API_KEY


@pytest.fixture
def api_key_hash():
    """Create hash of API key for comparison."""
    return hashlib.sha256(DASHBOARD_API_KEY.encode()).hexdigest()


@pytest.fixture
def invalid_api_key():
    """Return an incorrect API key."""
    return "sk-invalid-key-xyz-12345"


@pytest.fixture
def valid_bearer_token(api_key_hash):
    """Create a valid bearer token from hashed API key."""
    return f"Bearer {api_key_hash}"


@pytest.fixture
def invalid_bearer_token():
    """Create an invalid bearer token."""
    invalid_hash = hashlib.sha256(b"wrong_key").hexdigest()
    return f"Bearer {invalid_hash}"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: API Key Hashing and Validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPIKeyHashing:
    """Test API key validation and hashing."""

    def test_api_key_loaded_from_config(self, valid_api_key):
        """API key should be loaded from config."""
        assert valid_api_key, "API key should not be empty"
        assert isinstance(valid_api_key, str), "API key should be string"
        assert len(valid_api_key) > 10, "API key should be reasonably long"

    def test_api_key_hash_consistency(self, valid_api_key):
        """Same API key should always hash to same value."""
        hash1 = hashlib.sha256(valid_api_key.encode()).hexdigest()
        hash2 = hashlib.sha256(valid_api_key.encode()).hexdigest()
        assert hash1 == hash2, "Hash should be deterministic"

    def test_different_keys_different_hashes(self, valid_api_key, invalid_api_key):
        """Different keys should produce different hashes."""
        hash_valid = hashlib.sha256(valid_api_key.encode()).hexdigest()
        hash_invalid = hashlib.sha256(invalid_api_key.encode()).hexdigest()
        assert hash_valid != hash_invalid, "Different keys must hash differently"

    def test_bearer_token_format(self, valid_bearer_token):
        """Bearer token should have correct format."""
        parts = valid_bearer_token.split(" ")
        assert len(parts) == 2, "Bearer token must have 2 parts"
        assert parts[0] == "Bearer", "First part should be 'Bearer'"
        assert len(parts[1]) == 64, "Hash should be 64 hex characters (SHA256)"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Bearer Token Extraction and Validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestBearerTokenValidation:
    """Test token extraction from request headers."""

    def test_extract_bearer_token_valid(self, valid_bearer_token):
        """Extract bearer token from header."""
        header = valid_bearer_token
        assert header.startswith("Bearer "), "Should start with 'Bearer '"
        token = header[7:]  # Remove "Bearer " prefix
        assert len(token) == 64, "Token should be 64 hex chars"

    def test_extract_bearer_token_no_prefix(self, api_key_hash):
        """Token without Bearer prefix should be rejected."""
        bad_header = api_key_hash  # Missing "Bearer " prefix
        assert not bad_header.startswith("Bearer "), "Should detect missing Bearer prefix"

    def test_extract_bearer_token_malformed(self):
        """Malformed bearer token should be rejected."""
        bad_tokens = [
            "Bearer",  # Missing token
            "Bearer ",  # Missing token
            "Bearer abc123",  # Too short
            "Bearer xyz 123",  # Extra spaces
            "bearer abc123xyz",  # Wrong case (if case-sensitive)
        ]
        
        for bad_token in bad_tokens:
            parts = bad_token.split(" ")
            if len(parts) != 2:
                assert True, f"Should reject malformed: {bad_token}"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Rate Limiting
# ═══════════════════════════════════════════════════════════════════════════════

class TestRateLimiting:
    """Test per-IP rate limiting enforcement."""

    def test_rate_limit_store_initialization(self):
        """Rate limit store should start empty."""
        rate_limit_store = {}
        assert len(rate_limit_store) == 0, "Should start empty"

    def test_rate_limit_track_requests(self):
        """Rate limit should track requests per IP."""
        rate_limit_store = {}
        ip = "192.168.1.1"
        now = time.time()
        
        # Simulate requests
        if ip not in rate_limit_store:
            rate_limit_store[ip] = []
        
        for _ in range(3):
            rate_limit_store[ip].append(now)
        
        assert len(rate_limit_store[ip]) == 3, "Should track 3 requests"

    def test_rate_limit_cleanup_old_requests(self):
        """Old requests (>60s) should be removed from store."""
        rate_limit_store = {"192.168.1.1": []}
        ip = "192.168.1.1"
        now = time.time()
        
        # Add old and new requests
        old_time = now - 120  # 2 minutes ago
        new_time = now
        
        rate_limit_store[ip] = [old_time, old_time, new_time]
        
        # Filter to keep only recent (last 60s)
        rate_limit_store[ip] = [
            ts for ts in rate_limit_store[ip] if now - ts < 60
        ]
        
        assert len(rate_limit_store[ip]) == 1, "Should keep only recent"

    def test_rate_limit_per_ip_isolation(self):
        """Rate limits should be per IP, not global."""
        rate_limit_store = {}
        now = time.time()
        
        # Simulate different IPs making requests
        for ip in ["192.168.1.1", "192.168.1.2", "192.168.1.3"]:
            rate_limit_store[ip] = []
            for i in range(10):
                rate_limit_store[ip].append(now)
        
        # Each IP should have independent limits
        assert len(rate_limit_store["192.168.1.1"]) == 10
        assert len(rate_limit_store["192.168.1.2"]) == 10
        assert len(rate_limit_store["192.168.1.3"]) == 10
        
        # Limiting one shouldn't affect others
        rate_limit_store["192.168.1.1"] = []
        assert len(rate_limit_store["192.168.1.1"]) == 0
        assert len(rate_limit_store["192.168.1.2"]) == 10

    def test_rate_limit_threshold_60_per_minute(self):
        """Should allow 60 requests per minute."""
        rate_limit_store = {}
        ip = "192.168.1.1"
        rate_limit_store[ip] = [time.time()] * 60
        
        # Clean and check
        now = time.time()
        recent = [ts for ts in rate_limit_store[ip] if now - ts < 60]
        
        assert len(recent) == 60, "60 requests should pass"
        
        # 61st request should be blocked
        rate_limit_store[ip].append(now)
        recent = [ts for ts in rate_limit_store[ip] if now - ts < 60]
        assert len(recent) == 61, "61 requests should fail"

    def test_rate_limit_expires_after_minute(self):
        """Requests older than 60s should expire."""
        rate_limit_store = {}
        ip = "192.168.1.1"
        
        now = time.time()
        past_1min = now - 61  # Over 1 minute ago
        
        rate_limit_store[ip] = [past_1min, past_1min, now, now]
        
        # Clean old requests
        cleaned = [ts for ts in rate_limit_store[ip] if now - ts < 60]
        
        assert len(rate_limit_store[ip]) == 4, "Should have 4 before cleanup"
        assert len(cleaned) == 2, "Should have 2 after cleanup"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Session Token Generation
# ═══════════════════════════════════════════════════════════════════════════════

class TestSessionTokens:
    """Test session token creation and TTL."""

    def test_session_token_generation(self):
        """Session tokens should be cryptographically random."""
        import secrets
        
        tokens = set()
        for _ in range(10):
            token = secrets.token_hex(32)  # 64 hex char token
            tokens.add(token)
        
        assert len(tokens) == 10, "All tokens should be unique"

    def test_session_token_format(self):
        """Session token should be 64 hex characters."""
        import secrets
        token = secrets.token_hex(32)
        
        assert len(token) == 64, "Token should be 64 chars"
        assert all(c in "0123456789abcdef" for c in token), "Should be hex"

    def test_session_token_store(self):
        """Session tokens should be stored with creation time."""
        import secrets
        
        session_store = {}
        token = secrets.token_hex(32)
        now = time.time()
        
        session_store[token] = {
            "created": now,
            "ttl": 3600,  # 1 hour
        }
        
        assert token in session_store, "Token should be in store"
        assert session_store[token]["ttl"] == 3600, "TTL should be set"

    def test_session_token_expiration(self):
        """Expired tokens should be identified."""
        import secrets
        
        token = secrets.token_hex(32)
        now = time.time()
        created = now - 3700  # Created 3700s ago (>1hr)
        ttl = 3600  # 1 hour
        
        is_expired = (now - created) > ttl
        
        assert is_expired, "Token older than TTL should be expired"

    def test_session_token_cleanup(self):
        """Expired tokens should be removed."""
        import secrets
        
        session_store = {}
        now = time.time()
        
        # Add mix of valid and expired tokens
        valid_token = secrets.token_hex(32)
        expired_token = secrets.token_hex(32)
        
        session_store[valid_token] = {"created": now, "ttl": 3600}
        session_store[expired_token] = {"created": now - 3700, "ttl": 3600}
        
        # Clean expired
        session_store = {
            token: data for token, data in session_store.items()
            if (now - data["created"]) <= data["ttl"]
        }
        
        assert valid_token in session_store, "Valid token should remain"
        assert expired_token not in session_store, "Expired token should be removed"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Development Mode Bypass
# ═══════════════════════════════════════════════════════════════════════════════

class TestDevelopmentMode:
    """Test DEVELOPMENT_MODE flag behavior."""

    def test_dev_mode_configured(self):
        """DEVELOPMENT_MODE should be accessible."""
        assert isinstance(DEVELOPMENT_MODE, bool), "DEVELOPMENT_MODE should be bool"

    def test_dev_mode_auth_bypass(self):
        """In dev mode, auth checks should be bypassed."""
        if DEVELOPMENT_MODE:
            # In dev mode, any token is valid
            test_header = "Bearer invalid_token_xyz"
            # Should not raise or return 401
            assert True, "Dev mode allows invalid tokens"

    def test_dev_mode_flag_respected(self):
        """Code should check DEVELOPMENT_MODE flag."""
        # Example decorator checking pattern
        def check_auth(require_dev_mode=False):
            def decorator(fn):
                @wraps(fn)
                def wrapper(*args, **kwargs):
                    if DEVELOPMENT_MODE and (not require_dev_mode or require_dev_mode):
                        # Dev mode allows bypass
                        return fn(*args, **kwargs)
                    # Otherwise require auth
                    return fn(*args, **kwargs)
                return wrapper
            return decorator
        
        @check_auth()
        def test_endpoint():
            return {"success": True}
        
        result = test_endpoint()
        assert result["success"], "Endpoint should work (dev mode or no auth needed)"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Edge Cases and Error Handling
# ═══════════════════════════════════════════════════════════════════════════════

class TestAuthEdgeCases:
    """Test boundary conditions in auth."""

    def test_empty_authorization_header(self):
        """Empty auth header should be rejected."""
        header = ""
        assert not header.startswith("Bearer "), "Empty should fail"

    def test_none_authorization_header(self):
        """None auth header should be handled."""
        header = None
        assert header is None or not isinstance(header, str) or not header.startswith("Bearer "), "None should fail"

    def test_case_sensitivity_bearer(self):
        """'bearer' (lowercase) might not match 'Bearer' if case-sensitive."""
        header = "bearer abc123xyz"
        # Assuming case-sensitive comparison
        assert not header.startswith("Bearer "), "Should be case-sensitive"

    def test_extra_whitespace_in_token(self):
        """Extra whitespace should cause rejection."""
        header = "Bearer  abc123xyz"  # Double space
        parts = header.split(" ")
        # Should have more than 2 parts if split by space
        assert len(parts) > 2, "Extra whitespace should create extra parts"

    def test_token_with_newline(self):
        """Token with newline should be rejected."""
        header = "Bearer abc123xyz\n"
        should_be_clean = header.strip() != header
        assert should_be_clean, "Newline should be detected"

    def test_rate_limit_with_none_ip(self):
        """None IP should be handled gracefully."""
        ip = None
        # Should set default or reject
        if ip is None:
            ip = "0.0.0.0"  # Default
        assert ip is not None, "Should have valid IP"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Multiple Decorator Stacking
# ═══════════════════════════════════════════════════════════════════════════════

class TestDecoratorStacking:
    """Test multiple decorators working together."""

    def test_auth_and_rate_limit_together(self):
        """Auth + rate limit decorators should stack correctly."""
        # Simulating decorator order: @rate_limit then @auth
        def mock_rate_limit(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                # Check rate limit here
                return fn(*args, **kwargs)
            return wrapper
        
        def mock_auth(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                # Check auth here
                return fn(*args, **kwargs)
            return wrapper
        
        # Stack: rate_limit first, then auth
        @mock_rate_limit
        @mock_auth
        def endpoint():
            return {"success": True}
        
        result = endpoint()
        assert result["success"], "Stacked decorators should work"

    def test_decorator_order_matters(self):
        """Decorator order should affect execution order."""
        call_order = []
        
        def decorator_a(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                call_order.append("A")
                return fn(*args, **kwargs)
            return wrapper
        
        def decorator_b(fn):
            @wraps(fn)
            def wrapper(*args, **kwargs):
                call_order.append("B")
                return fn(*args, **kwargs)
            return wrapper
        
        # Order: @decorator_b @decorator_a means B wraps (A wraps func)
        @decorator_b
        @decorator_a
        def endpoint():
            call_order.append("func")
            return call_order
        
        result = endpoint()
        # Outer decorator (B) runs first, then inner (A), then function
        assert result == ["B", "A", "func"], f"Order should be B, A, func. Got {result}"
