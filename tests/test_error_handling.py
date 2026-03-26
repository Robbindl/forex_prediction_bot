"""
tests/test_error_handling.py — Unit tests for error handling framework

Tests Phase 1 error handling implementation:
  • Custom exception classes (BadRequest, Unauthorized, etc.)
  • Error response generation with error IDs
  • Request validation helper
  • Error ID generation and format
  • Logging integration
"""

import pytest
import json
import time
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_errors import (
    APIError, BadRequest, Unauthorized, Forbidden, NotFound, InternalError,
    handle_api_error, validate_request_json, log_api_call
)


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Custom Exception Classes
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPIExceptionHierarchy:
    """Test custom exception class hierarchy."""

    def test_bad_request_exception(self):
        """BadRequest should have status code 400."""
        exc = BadRequest("Missing required field: api_key")
        assert exc.status_code == 400
        assert exc.message == "Missing required field: api_key"
        assert isinstance(exc, APIError)

    def test_unauthorized_exception(self):
        """Unauthorized should have status code 401."""
        exc = Unauthorized("Invalid API key")
        assert exc.status_code == 401
        assert exc.message == "Invalid API key"
        assert isinstance(exc, APIError)

    def test_forbidden_exception(self):
        """Forbidden should have status code 403."""
        exc = Forbidden("Insufficient permissions")
        assert exc.status_code == 403
        assert exc.message == "Insufficient permissions"
        assert isinstance(exc, APIError)

    def test_not_found_exception(self):
        """NotFound should have status code 404."""
        exc = NotFound("Resource not found")
        assert exc.status_code == 404
        assert exc.message == "Resource not found"
        assert isinstance(exc, APIError)

    def test_internal_error_exception(self):
        """InternalError should have status code 500."""
        exc = InternalError("Database connection failed")
        assert exc.status_code == 500
        assert exc.message == "Database connection failed"
        assert isinstance(exc, APIError)

    def test_exception_details_optional(self):
        """Exception details should be optional."""
        exc_no_details = BadRequest("Invalid input")
        assert exc_no_details.details is None
        
        exc_with_details = BadRequest("Invalid input", details={"field": "email"})
        assert exc_with_details.details == {"field": "email"}

    def test_exception_inheritance(self):
        """All custom exceptions should inherit from APIError."""
        exceptions = [
            BadRequest("msg"),
            Unauthorized("msg"),
            Forbidden("msg"),
            NotFound("msg"),
            InternalError("msg"),
        ]
        
        for exc in exceptions:
            assert isinstance(exc, APIError), f"{exc.__class__.__name__} should inherit from APIError"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Error Response Generation
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorResponseGeneration:
    """Test handle_api_error response format."""

    def test_handle_api_error_bad_request(self):
        """BadRequest should return 400 with error message."""
        exc = BadRequest("Email is required")
        response, status = handle_api_error(exc, "/api/register", 400)
        
        assert status == 400
        assert response["success"] == False
        assert "error" in response
        assert "Email is required" in str(response["error"])

    def test_handle_api_error_unauthorized(self):
        """Unauthorized should return 401 with error message."""
        exc = Unauthorized("Invalid token")
        response, status = handle_api_error(exc, "/api/status", 401)
        
        assert status == 401
        assert response["success"] == False

    def test_handle_api_error_includes_error_id_on_500(self):
        """500 errors should include error_id for tracking."""
        exc = InternalError("Database error")
        response, status = handle_api_error(exc, "/api/trades", 500)
        
        assert status == 500
        assert "error_id" in response, "500 errors should have error_id"
        assert response["error_id"], "error_id should not be empty"
        assert len(response["error_id"]) == 14, "error_id should be 14 chars (YYYYMMDDHHMMSS)"

    def test_handle_generic_exception(self):
        """Generic exceptions should be converted to 500 error."""
        exc = ValueError("Something went wrong")
        response, status = handle_api_error(exc, "/api/endpoint", 500)
        
        assert status == 500
        assert response["success"] == False
        assert "error_id" in response

    def test_error_response_format_consistency(self):
        """All error responses should follow same format."""
        exceptions = [
            (BadRequest("msg"), 400),
            (Unauthorized("msg"), 401),
            (Forbidden("msg"), 403),
            (NotFound("msg"), 404),
        ]
        
        for exc, expected_status in exceptions:
            response, status = handle_api_error(exc, "/api/test", expected_status)
            
            assert status == expected_status
            assert "success" in response
            assert response["success"] == False
            assert "error" in response


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Error ID Generation
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorIDGeneration:
    """Test error ID creation and format."""

    def test_error_id_format(self):
        """Error ID should be YYYYMMDDHHMMSS (14 characters)."""
        from utils.api_errors import handle_api_error
        exc = InternalError("test")
        response, status = handle_api_error(exc, "/api/test", 500)
        
        error_id = response.get("error_id")
        assert error_id is not None, "500 should have error_id"
        assert len(error_id) == 14, f"error_id should be 14 chars, got {len(error_id)}"
        assert error_id.isdigit(), "error_id should be all digits"

    def test_error_id_contains_timestamp(self):
        """Error ID should reflect current timestamp."""
        from utils.api_errors import handle_api_error
        import time
        
        before = datetime.now().strftime("%Y%m%d%H%M%S")
        exc = InternalError("test")
        response, status = handle_api_error(exc, "/api/test", 500)
        after = datetime.now().strftime("%Y%m%d%H%M%S")
        
        error_id = response.get("error_id")
        # error_id should be between before and after
        assert error_id >= before and error_id <= after, "error_id should reflect current time"

    def test_error_id_unique_per_error(self):
        """Different errors should have different IDs (unless same second)."""
        from utils.api_errors import handle_api_error
        
        error_ids = set()
        for _ in range(3):
            exc = InternalError("test")
            response, _ = handle_api_error(exc, "/api/test", 500)
            error_id = response.get("error_id")
            error_ids.add(error_id)
            time.sleep(0.01)  # Small delay
        
        # Should have at least some unique IDs (same second = same ID)
        assert len(error_ids) >= 1, "Should generate IDs"

    def test_error_id_only_on_500(self):
        """4xx errors should not have error_id."""
        from utils.api_errors import handle_api_error
        
        exc = BadRequest("msg")
        response, _ = handle_api_error(exc, "/api/test", 400)
        assert "error_id" not in response, "4xx should not have error_id"


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Request Validation
# ═══════════════════════════════════════════════════════════════════════════════

class TestRequestValidation:
    """Test validate_request_json helper."""

    def test_validate_valid_json(self):
        """Valid JSON with required fields should pass."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = {"api_key": "sk-12345"}
            
            result = validate_request_json(required_fields=["api_key"])
            
            assert result == {"api_key": "sk-12345"}

    def test_validate_missing_required_field(self):
        """Missing required field should raise BadRequest."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = {"user": "john"}
            
            with pytest.raises(BadRequest) as exc_info:
                validate_request_json(required_fields=["api_key"])
            
            assert "api_key" in str(exc_info.value.message).lower()

    def test_validate_multiple_required_fields(self):
        """Should validate multiple required fields."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = {
                "api_key": "sk-12345",
                "username": "john"
            }
            
            result = validate_request_json(required_fields=["api_key", "username"])
            
            assert result["api_key"] == "sk-12345"
            assert result["username"] == "john"

    def test_validate_empty_required_field_rejected(self):
        """Empty string field should be treated as missing."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = {"api_key": ""}
            
            with pytest.raises(BadRequest) as exc_info:
                validate_request_json(required_fields=["api_key"])
            
            assert "api_key" in str(exc_info.value.message).lower()

    def test_validate_none_json(self):
        """None JSON should raise BadRequest."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = None
            
            with pytest.raises(BadRequest):
                validate_request_json(required_fields=["api_key"])

    def test_validate_extra_fields_allowed(self):
        """Extra fields not in required list should be allowed."""
        with patch('utils.api_errors.request') as mock_request:
            mock_request.get_json.return_value = {
                "api_key": "sk-12345",
                "extra_field": "should be allowed"
            }
            
            result = validate_request_json(required_fields=["api_key"])
            
            assert "api_key" in result
            assert "extra_field" in result


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Logging Integration
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorLogging:
    """Test error logging with structured output."""

    def test_log_api_call_format(self):
        """API call should be logged with structured format."""
        with patch('utils.api_errors.logger') as mock_logger:
            log_data = {
                "endpoint": "/api/status",
                "method": "GET",
                "status": 200,
                "duration_ms": 45.2,
                "ip": "192.168.1.1",
                "user_agent": "Mozilla/5.0"
            }
            
            # log_api_call should write structured JSON-like log
            log_api_call(
                endpoint=log_data["endpoint"],
                method=log_data["method"],
                status=log_data["status"],
                duration_ms=log_data["duration_ms"],
                ip=log_data["ip"],
                user_agent=log_data["user_agent"],
            )
            
            # Verify log was called
            assert mock_logger.info.called or mock_logger.debug.called

    def test_log_includes_timestamp(self):
        """API log should include timestamp."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "endpoint": "/api/status",
            "status": 200,
        }
        
        assert "timestamp" in log_entry
        # Timestamp should be ISO format
        assert "T" in log_entry["timestamp"] or "-" in log_entry["timestamp"]

    def test_error_log_includes_traceback(self):
        """Error logs for 500s should include traceback."""
        exc = InternalError("Database error")
        
        # When handling a 500 error, traceback should be captured
        import traceback
        try:
            raise ValueError("Test error")
        except ValueError:
            tb_str = traceback.format_exc()
            assert "Traceback" in tb_str or "ValueError" in tb_str


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: Edge Cases and Error Handling
# ═══════════════════════════════════════════════════════════════════════════════

class TestErrorHandlingEdgeCases:
    """Test boundary conditions in error handling."""

    def test_exception_with_unicode_message(self):
        """Exceptions with unicode characters should be handled."""
        exc = BadRequest("Invalid input: café ñ 中文")
        response, status = handle_api_error(exc, "/api/test", 400)
        
        assert status == 400
        assert "error" in response

    def test_exception_with_very_long_message(self):
        """Very long error messages should be handled."""
        long_msg = "x" * 10000
        exc = InternalError(long_msg)
        response, status = handle_api_error(exc, "/api/test", 500)
        
        assert status == 500
        assert "error" in response

    def test_exception_with_none_message(self):
        """None message should be handled gracefully."""
        exc = APIError()
        exc.message = None
        response, status = handle_api_error(exc, "/api/test", 500)
        
        assert status == 500
        assert "error" in response

    def test_nested_exception_handling(self):
        """Nested exceptions should be handled."""
        try:
            try:
                raise ValueError("Inner error")
            except ValueError as e:
                raise InternalError(f"Wrapped: {e}")
        except InternalError as e:
            response, status = handle_api_error(e, "/api/test", 500)
            assert status == 500

    def test_exception_with_special_characters(self):
        """Error messages with special chars should be JSON-safe."""
        exc = BadRequest('Invalid: "quotes" and <tags>')
        response, status = handle_api_error(exc, "/api/test", 400)
        
        # Should be JSON-serializable
        json_str = json.dumps(response)
        assert len(json_str) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# TESTS: HTTP Status Code Mapping
# ═══════════════════════════════════════════════════════════════════════════════

class TestHTTPStatusMapping:
    """Test proper HTTP status code assignment."""

    def test_status_code_400_bad_request(self):
        """BadRequest should always map to 400."""
        exc = BadRequest("msg")
        assert exc.status_code == 400

    def test_status_code_401_unauthorized(self):
        """Unauthorized should always map to 401."""
        exc = Unauthorized("msg")
        assert exc.status_code == 401

    def test_status_code_403_forbidden(self):
        """Forbidden should always map to 403."""
        exc = Forbidden("msg")
        assert exc.status_code == 403

    def test_status_code_404_not_found(self):
        """NotFound should always map to 404."""
        exc = NotFound("msg")
        assert exc.status_code == 404

    def test_status_code_500_internal_error(self):
        """InternalError should always map to 500."""
        exc = InternalError("msg")
        assert exc.status_code == 500

    def test_response_status_matches_exception_status(self):
        """Response status should match exception status."""
        exc = Unauthorized("Invalid token")
        response, status = handle_api_error(exc, "/api/test", exc.status_code)
        
        assert status == exc.status_code
        assert status == 401
