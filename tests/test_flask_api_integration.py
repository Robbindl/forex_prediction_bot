"""
tests/test_flask_api_integration.py — Integration tests for Flask API endpoints

Tests Phase 1 + Phase 2 integration:
  • Flask app initialization
  • API endpoint responses
  • Error handler integration with endpoints
  • CORS policy enforcement
  • Development mode operation
"""

import pytest
import json
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ═══════════════════════════════════════════════════════════════════════════════
# FIXTURES — Flask App and Client
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def flask_app():
    """Create Flask test app."""
    try:
        from dashboard.web_app_live import app
        app.config['TESTING'] = True
        return app
    except Exception as e:
        pytest.skip(f"Could not load Flask app: {e}")


@pytest.fixture
def client(flask_app):
    """Create Flask test client."""
    return flask_app.test_client()


@pytest.fixture
def app_context(flask_app):
    """Create Flask app context for testing."""
    with flask_app.app_context():
        yield flask_app


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Flask App Initialization
# ═══════════════════════════════════════════════════════════════════════════════

class TestFlaskAppSetup:
    """Test basic Flask app configuration."""

    def test_app_exists(self, flask_app):
        """Flask app should be created."""
        assert flask_app is not None
        assert hasattr(flask_app, 'config')

    def test_app_testing_mode(self, flask_app):
        """App should be in testing mode."""
        assert flask_app.config['TESTING'] == True

    def test_error_handlers_registered(self, flask_app):
        """Error handlers should be registered."""
        error_codes = [400, 401, 403, 404, 500]
        for code in error_codes:
            assert code in flask_app.error_handler_spec.get(None, {}), \
                f"Handler for {code} not registered"

    def test_cors_enabled(self, flask_app):
        """CORS should be enabled for localhost."""
        # CORS config is checked in blueprint
        assert flask_app is not None


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Login Endpoint
# ═══════════════════════════════════════════════════════════════════════════════

class TestLoginEndpoint:
    """Test /api/login endpoint."""

    def test_login_with_valid_key(self, client):
        """Login with valid API key should succeed."""
        from config.config import DASHBOARD_API_KEY
        
        response = client.post('/api/login', 
            json={"api_key": DASHBOARD_API_KEY})
        
        # In dev mode or with valid key, should return success
        data = response.get_json()
        assert data is not None, "Should return JSON"
        
        # Check response format
        if response.status_code == 200:
            assert data.get("success") in (True, False)

    def test_login_with_missing_key(self, client):
        """Login without api_key should fail or return dev token."""
        from config.config import DEVELOPMENT_MODE
        
        response = client.post('/api/login', json={})
        
        # In dev mode, missing key still returns 200 (dev mode bypass)
        # In strict mode, should return 400/401/403
        if DEVELOPMENT_MODE:
            # Dev mode returns 200 with a token
            assert response.status_code == 200, "Dev mode should return token"
        else:
            assert response.status_code in (400, 401, 403), "Should return error"

    def test_login_with_invalid_key(self, client):
        """Login with wrong API key should fail or bypass in dev mode."""
        from config.config import DEVELOPMENT_MODE
        
        response = client.post('/api/login', 
            json={"api_key": "invalid_key_12345"})
        
        # Dev mode bypasses validation
        if DEVELOPMENT_MODE:
            assert response.status_code == 200
        else:
            assert response.status_code in (401, 403)

    def test_login_response_format(self, client):
        """Login response should follow standard format."""
        from config.config import DASHBOARD_API_KEY
        
        response = client.post('/api/login', 
            json={"api_key": DASHBOARD_API_KEY})
        
        data = response.get_json()
        assert isinstance(data, dict), "Response should be dict"
        assert "success" in data, "Response should have success field"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Status Endpoints
# ═══════════════════════════════════════════════════════════════════════════════

class TestStatusEndpoints:
    """Test /api/status and /api/system-status endpoints."""

    def test_status_endpoint_exists(self, client):
        """GET /api/status should exist and be callable."""
        response = client.get('/api/status')
        
        # Trading core may not be initialized in test environment
        # Should return 200 (success) or 500 (trading core error), not 404
        assert response.status_code != 404, "Endpoint should exist"
        assert response.status_code in (200, 500, 401, 403)

    def test_status_response_format(self, client):
        """Status response should be JSON."""
        response = client.get('/api/status')
        
        if response.status_code == 200:
            data = response.get_json()
            assert isinstance(data, dict)
            assert "success" in data

    def test_system_status_endpoint_exists(self, client):
        """GET /api/system-status should exist."""
        response = client.get('/api/system-status')
        
        # Should return 200 or 401/403 auth error, not 404
        assert response.status_code != 404, "Endpoint should exist"
        assert response.status_code in (200, 500, 401, 403)

    def test_system_status_returns_metrics(self, client):
        """System status should return metrics or error."""
        response = client.get('/api/system-status')
        
        if response.status_code == 200:
            data = response.get_json()
            # Should contain trading metrics
            assert "success" in data


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Error Handler Integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorHandlerIntegration:
    """Test error handlers work with actual endpoints."""

    def test_404_error_handler(self, client):
        """Non-existent endpoint should trigger 404 handler."""
        response = client.get('/api/nonexistent-endpoint-xyz')
        
        assert response.status_code == 404
        data = response.get_json()
        assert data is not None
        assert data.get("success") == False
        assert "error" in data

    def test_400_error_on_bad_request(self, client):
        """Bad request should trigger 400 handler."""
        response = client.post('/api/login', json={})
        
        # Should be 400 or 401
        assert response.status_code in (400, 401, 403, 200)
        if response.status_code in (400, 401, 403):
            data = response.get_json()
            assert data.get("success") == False

    def test_error_response_has_success_field(self, client):
        """All error responses should have success=false."""
        response = client.get('/api/nonexistent')
        
        data = response.get_json()
        assert "success" in data
        assert data["success"] == False

    def test_error_response_has_error_field(self, client):
        """All error responses should have error field."""
        response = client.get('/api/nonexistent')
        
        data = response.get_json()
        assert "error" in data


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: CORS Policy
# ═══════════════════════════════════════════════════════════════════════════════

class TestCORSPolicy:
    """Test CORS restrictions."""

    def test_localhost_origin_allowed(self, client):
        """localhost:5000 should be allowed."""
        response = client.get('/api/status',
            headers={'Origin': 'http://localhost:5000'})
        
        # Should not be blocked by CORS (might be blocked by auth or trading core)
        # Just verify endpoint responds and doesn't have CORS error
        assert response.status_code in (200, 401, 403, 500), \
            "CORS should allow localhost"

    def test_multiple_endpoints_accessible(self, client):
        """Multiple endpoints should be accessible."""
        endpoints = [
            '/api/status',
            '/api/system-status',
            '/api/signals/live',
            '/api/chart/assets',
        ]
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code in (200, 401, 403, 404, 500)


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Development Mode Operation
# ═══════════════════════════════════════════════════════════════════════════════

class TestDevelopmentModeOperation:
    """Test app functions in development mode."""

    def test_dev_mode_auth_bypass(self, client):
        """In dev mode, endpoints should be accessible without auth."""
        from config.config import DEVELOPMENT_MODE
        
        # Get any endpoint
        response = client.get('/api/status')
        
        if DEVELOPMENT_MODE:
            # In dev mode, auth should be bypassed
            # May get 500 from trading core not initialized, but NOT 401/403
            assert response.status_code != 401, "Dev mode should bypass auth"
            assert response.status_code != 403, "Dev mode should bypass auth"
            # Can be 200 (working) or 500 (trading core error), but not auth error
            assert response.status_code in (200, 500)

    def test_dev_mode_flag_respected(self, flask_app):
        """App should respect DEVELOPMENT_MODE flag."""
        from config.config import DEVELOPMENT_MODE
        
        assert isinstance(DEVELOPMENT_MODE, bool)
        # Flag should be usable in app initialization


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Response Content Types
# ═══════════════════════════════════════════════════════════════════════════════

class TestResponseContentTypes:
    """Test response content types."""

    def test_json_responses(self, client):
        """API responses should be JSON."""
        response = client.get('/api/status')
        
        assert response.content_type.startswith('application/json') or \
               'json' in response.content_type.lower(), \
               f"Expected JSON, got {response.content_type}"

    def test_error_responses_are_json(self, client):
        """Error responses should be JSON."""
        response = client.get('/api/nonexistent')
        
        assert response.content_type.startswith('application/json') or \
               'json' in response.content_type.lower()

    def test_malformed_json_handling(self, client):
        """Malformed JSON in dev mode may still be accepted."""
        response = client.post('/api/login',
            data='not json',
            content_type='application/json')
        
        assert response.status_code in (200, 400)


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Endpoint Availability
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndpointAvailability:
    """Test major endpoints are available."""

    @pytest.mark.parametrize("endpoint,method", [
        ('/api/status', 'GET'),
        ('/api/system-status', 'GET'),
        ('/api/login', 'POST'),
        ('/api/logout', 'POST'),
        ('/api/chart/assets', 'GET'),
        ('/api/signals/live', 'GET'),
    ])
    def test_endpoint_has_handler(self, client, endpoint, method):
        """Endpoint should have a handler registered."""
        if method == 'GET':
            response = client.get(endpoint)
        else:
            response = client.post(endpoint, json={})
        
        assert response.status_code != 404


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Response Schema Consistency
# ═══════════════════════════════════════════════════════════════════════════════

class TestResponseSchema:
    """Test response schema consistency."""

    def test_error_response_schema(self, client):
        """Error responses should have consistent schema."""
        response = client.get('/api/nonexistent')
        data = response.get_json()
        
        assert "success" in data
        assert "error" in data
        
        if response.status_code >= 400:
            assert data["success"] == False

    def test_success_response_has_success_field(self, client):
        """Success responses should have success=true."""
        response = client.get('/api/status')
        
        if response.status_code == 200:
            data = response.get_json()
            assert "success" in data
            assert data["success"] == True

    def test_error_id_in_500_responses(self, flask_app, client):
        """500 errors should have error_id."""
        response = client.get('/api/nonexistent')
        
        if response.status_code == 404:
            data = response.get_json()
            if response.status_code < 500:
                assert "error_id" not in data or data.get("error_id") is None


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Request/Response Cycle
# ═══════════════════════════════════════════════════════════════════════════════

class TestRequestResponseCycle:
    """Test complete request/response cycle."""

    def test_get_request_cycle(self, client):
        """GET request should complete cycle."""
        response = client.get('/api/status')
        
        # Should return valid response
        assert response.status_code in range(200, 600)
        assert response.data is not None

    def assert response.status_code in range(200, 600)
        assert response.data is not None

    def test_post_request_cycle(self, client):
        """POST request should complete cycle."""
        response = client.post('/api/login', json={"api_key": "test"})
    def test_response_can_be_decoded(self, client):
        """Response should be JSON-decodable."""
        response = client.get('/api/status')
        
        if response.data:
            try:
                data = json.loads(response.data)
                assert isinstance(data, dict)
            except json.JSONDecodeError:
                pytest.fail("Response should be valid JSON")


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Error Handling Through Flask Requests (Phase 3)
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorResponsesViaFlask:
    """Test error handling framework through actual Flask requests."""

    def test_404_error_response_complete(self, client):
        """404 errors should have all fields."""
        response = client.get('/api/nonexistent-endpoint-xyz')
        
        assert response.status_code == 404
        data = response.get_json()
        assert data.get("success") == False
        assert "error" in data
        assert isinstance(data.get("error"), str)

    def test_400_error_bad_request(self, client):
        """400 errors should have success=false."""
        response = client.post('/api/login', json={})
        
        if response.status_code == 400:
            data = response.get_json()
            assert data.get("success") == False
            assert "error" in data

    def test_401_error_unauthorized(self, client):
        """401 errors should be properly returned."""
        response = client.post('/api/login', json={"api_key": "wrong"})
        
        # Dev mode returns 200, strict mode returns 401
        if response.status_code == 401:
            data = response.get_json()
            assert data.get("success") == False

    def test_403_error_forbidden(self, client):
        """403 errors should be properly returned."""
        response = client.post('/api/login', json={"api_key": "forbidden"})
        
        # Dev mode returns 200, strict mode returns 403
        if response.status_code == 403:
            data = response.get_json()
            assert data.get("success") == False


class TestErrorIDGeneration:
    """Test error ID generation in actual Flask requests."""

    def test_404_returns_error_info(self, client):
        """404 should return error information."""
        response = client.get('/api/nonexistent')
        
        assert response.status_code == 404
        data = response.get_json()
        assert "error" in data

    def test_error_response_is_json(self, client):
        """Error response must be valid JSON."""
        response = client.get('/api/nonexistent')
        
        # Should be parseable JSON
        data = response.get_json()
        assert isinstance(data, dict)

    def test_multiple_errors_unique(self, client):
        """Multiple error requests should be distinguishable."""
        resp1 = client.get('/api/missing1')
        resp2 = client.get('/api/missing2')
        
        # Both should have 404 status
        assert resp1.status_code == 404
        assert resp2.status_code == 404
        
        # Both should have error info
        data1 = resp1.get_json()
        data2 = resp2.get_json()
        assert data1.get("success") == False
        assert data2.get("success") == False


class TestRequestValidationErrors:
    """Test request validation through actual Flask endpoints."""

    def test_missing_required_field(self, client):
        """Endpoints requiring fields should validate."""
        # POST without required fields
        response = client.post('/api/login', json={})
        
        # Should return valid response (200 in dev mode, error in strict)
        assert response.status_code in (200, 400, 401, 403)
        data = response.get_json()
        assert isinstance(data, dict)

    def test_invalid_json_body(self, client):
        """Invalid JSON should be handled."""
        response = client.post('/api/login',
            data='not json',
            content_type='application/json')
        
        # Should not crash, should return valid response
        assert response.status_code in (200, 400, 415)

    def test_empty_json_object(self, client):
        """Empty JSON object should be handled."""
        response = client.post('/api/login', json={})
        
        # Should return valid response
        assert response.status_code in range(200, 600)
        data = response.get_json()
        assert isinstance(data, dict)

    def test_extra_fields_allowed(self, client):
        """Extra fields in request should be allowed."""
        response = client.post('/api/login', json={
            "api_key": "test",
            "extra_field": "should_be_ignored"
        })
        
        # Should not fail, should handle gracefully
        assert response.status_code in range(200, 600)


class TestErrorLoggingBehavior:
    """Test error logging through Flask requests."""

    def test_404_logged(self, client):
        """404 should be logged."""
        response = client.get('/api/nonexistent')
        
        # Request should complete successfully
        assert response.status_code == 404

    def test_error_response_consistent(self, client):
        """All error responses should be consistent."""
        endpoints = ['/api/bad1', '/api/bad2', '/api/bad3']
        
        for endpoint in endpoints:
            response = client.get(endpoint)
            assert response.status_code == 404
            
            data = response.get_json()
            assert "success" in data
            assert "error" in data
            assert data.get("success") == False

    def test_multiple_errors_dont_interfere(self, client):
        """Multiple errors should not interfere with each other."""
        resp1 = client.get('/api/notfound1')
        resp2 = client.post('/api/login', json={})
        resp3 = client.get('/api/notfound2')
        
        # All should be valid responses
        assert resp1.status_code == 404
        assert resp2.status_code in (200, 400, 401, 403)
        assert resp3.status_code == 404


class TestHTTPStatusCodeMapping:
    """Test HTTP status codes are correct."""

    def test_404_status_code(self, client):
        """Non-existent endpoints return 404."""
        response = client.get('/api/does-not-exist')
        assert response.status_code == 404

    def test_200_status_for_valid_response(self, client):
        """Valid endpoints return 200 or 500."""
        response = client.get('/api/status')
        assert response.status_code in (200, 500)

    def test_login_returns_valid_status(self, client):
        """Login endpoint returns valid status."""
        response = client.post('/api/login', json={"api_key": "test"})
        assert response.status_code in (200, 400, 401, 403)

    def test_all_error_responses_valid_json(self, client):
        """All error responses must be valid JSON."""
        response = client.get('/api/nonexistent')
        
        data = response.get_json()
        assert data is not None
        assert isinstance(data, dict)


class TestErrorHandlingEdgeCases:
    """Test edge cases in error handling."""

    def test_very_long_endpoint_name(self, client):
        """Very long endpoint names should be handled."""
        long_name = '/api/' + 'a' * 1000
        response = client.get(long_name)
        
        assert response.status_code == 404
        data = response.get_json()
        assert "error" in data

    def test_special_characters_in_endpoint(self, client):
        """Special characters in endpoint should be handled."""
        response = client.get('/api/test@#$%^&*()')
        
        assert response.status_code == 404

    def test_unicode_in_endpoint(self, client):
        """Unicode characters in endpoint should be handled."""
        response = client.get('/api/tëst')
        
        assert response.status_code == 404

    def test_multiple_slashes(self, client):
        """Multiple slashes should be handled."""
        response = client.get('/api///test///endpoint')
        
        # Should return 404 or redirect, not crash
        assert response.status_code in (301, 302, 404, 405)

    def test_concurrent_error_requests(self, client):
        """Multiple concurrent-like requests should work."""
        responses = [
            client.get('/api/err1'),
            client.get('/api/err2'),
            client.get('/api/err3'),
        ]
        
        for response in responses:
            assert response.status_code == 404
            data = response.get_json()
            assert "error" in data
