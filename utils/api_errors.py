"""
API error handling and structured logging utilities.
Provides consistent error responses and logging across all endpoints.
"""

import json
import logging
import traceback
from datetime import datetime
from flask import jsonify, request
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Base class for API errors."""
    
    def __init__(self, message: str, status_code: int = 500, details: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.details = details or {}


class BadRequest(APIError):
    """400 Bad Request."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, 400, details)


class Unauthorized(APIError):
    """401 Unauthorized."""
    def __init__(self, message: str = "Unauthorized", details: Optional[Dict] = None):
        super().__init__(message, 401, details)


class Forbidden(APIError):
    """403 Forbidden."""
    def __init__(self, message: str = "Forbidden", details: Optional[Dict] = None):
        super().__init__(message, 403, details)


class NotFound(APIError):
    """404 Not Found."""
    def __init__(self, message: str = "Not Found", details: Optional[Dict] = None):
        super().__init__(message, 404, details)


class InternalError(APIError):
    """500 Internal Server Error."""
    def __init__(self, message: str = "Internal Server Error", details: Optional[Dict] = None):
        super().__init__(message, 500, details)


def log_api_call(endpoint: str, method: str, status: int, duration_ms: float, error: Optional[str] = None):
    """Log structured API call with timing and status."""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "endpoint": endpoint,
        "method": method,
        "status": status,
        "duration_ms": round(duration_ms, 2),
        "remote_addr": request.remote_addr,
        "user_agent": request.headers.get("User-Agent", "unknown")[:100],
    }
    
    if error:
        log_entry["error"] = error
        logger.error(json.dumps(log_entry))
    elif status >= 500:
        logger.error(json.dumps(log_entry))
    elif status >= 400:
        logger.warning(json.dumps(log_entry))
    else:
        logger.info(json.dumps(log_entry))


def handle_api_error(error: Exception, endpoint: str = "unknown", status_code: int = 500) -> tuple:
    """Convert exception to JSON response with logging."""
    
    if isinstance(error, APIError):
        response = {
            "success": False,
            "error": error.message,
            "status": error.status_code,
        }
        if error.details:
            response["details"] = error.details
        log_api_call(endpoint, request.method, error.status_code, 0, error.message)
        return jsonify(response), error.status_code
    
    # Unexpected error
    error_id = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:14]
    response = {
        "success": False,
        "error": "Internal Server Error",
        "error_id": error_id,
        "message": str(error)[:200],
    }
    
    logger.error(f"[{error_id}] Unexpected error in {endpoint}: {str(error)}")
    logger.error(f"[{error_id}] Traceback: {traceback.format_exc()}")
    log_api_call(endpoint, request.method, 500, 0, f"Error ID: {error_id}")
    
    return jsonify(response), 500


def validate_request_json(required_fields: list = None) -> Dict:
    """Validate and extract JSON from request body."""
    try:
        data = request.get_json()
        if data is None:
            raise BadRequest("Request body must be valid JSON")
        
        if required_fields:
            missing = [f for f in required_fields if f not in data]
            if missing:
                raise BadRequest(
                    "Missing required fields",
                    {"missing_fields": missing}
                )
        
        return data
    except BadRequest:
        raise
    except Exception as e:
        raise BadRequest(f"Invalid JSON: {str(e)}")
