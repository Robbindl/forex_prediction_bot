"""
Advanced error handling with retries and fallbacks
"""
import functools
import time
import asyncio
from typing import Type, Union, Tuple, Optional, Callable, Any
from datetime import datetime, timedelta
import random
from logger import logger

class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retry_on = retry_on


class CircuitBreaker:
    """Circuit breaker pattern to prevent repeated failures"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        name: str = "default"
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def record_failure(self) -> None:
        """Record a failure and potentially open the circuit"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker {self.name} OPEN after {self.failure_count} failures")
    
    def record_success(self) -> None:
        """Record a success and reset the circuit"""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            logger.info(f"Circuit breaker {self.name} CLOSED after successful recovery")
    
    def can_execute(self) -> bool:
        """Check if execution is allowed"""
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            # Check if recovery timeout has passed
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.recovery_timeout:
                    self.state = "HALF_OPEN"
                    logger.info(f"Circuit breaker {self.name} HALF_OPEN, trying recovery")
                    return True
            return False
        
        # HALF_OPEN state - allow one trial
        return True


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_on: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    fallback: Optional[Callable] = None,
    circuit_breaker: Optional[CircuitBreaker] = None
):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_retries: Maximum number of retries
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exponential_base: Base for exponential backoff
        jitter: Add random jitter to delay
        retry_on: Exception types to retry on
        fallback: Fallback function if all retries fail
        circuit_breaker: Circuit breaker instance
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Check circuit breaker
            if circuit_breaker and not circuit_breaker.can_execute():
                logger.warning(f"Circuit breaker {circuit_breaker.name} is OPEN, skipping {func.__name__}")
                if fallback:
                    return fallback(*args, **kwargs)
                return None
            
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        # Calculate delay with exponential backoff
                        delay = min(
                            base_delay * (exponential_base ** (attempt - 1)),
                            max_delay
                        )
                        
                        # Add jitter if enabled
                        if jitter:
                            delay = delay * (0.5 + random.random())
                        
                        logger.debug(f"Retry {attempt}/{max_retries} for {func.__name__} after {delay:.2f}s")
                        time.sleep(delay)
                    
                    result = func(*args, **kwargs)
                    
                    # Record success in circuit breaker
                    if circuit_breaker:
                        circuit_breaker.record_success()
                    
                    return result
                    
                except retry_on as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"All {max_retries} retries failed for {func.__name__}: {e}")
                        
                        # Record failure in circuit breaker
                        if circuit_breaker:
                            circuit_breaker.record_failure()
                        
                        # Call fallback if provided
                        if fallback:
                            try:
                                logger.info(f"Using fallback for {func.__name__}")
                                return fallback(*args, **kwargs)
                            except Exception as fb_e:
                                logger.error(f"Fallback also failed: {fb_e}")
                        
                        raise
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
            
            return None
        
        return wrapper
    return decorator


def safe_execute(
    error_message: str = "Operation failed",
    default_value: Any = None,
    log_level: str = "error"
):
    """
    Decorator to safely execute a function with error handling
    
    Args:
        error_message: Message to log on error
        default_value: Value to return on error
        log_level: Log level for errors ('error', 'warning', 'debug')
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                log_func = getattr(logger, log_level.lower(), logger.error)
                log_func(f"{error_message}: {e}")
                
                # Log full traceback in debug mode
                logger.debug(f"Traceback for {func.__name__}:", exc_info=True)
                
                return default_value
        return wrapper
    return decorator


class APIErrorHandler:
    """Specialized error handler for API calls"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.circuit_breaker = CircuitBreaker(name=service_name)
        self.failure_counts = {}
    
    @retry(
        max_retries=3,
        base_delay=2.0,
        retry_on=(ConnectionError, TimeoutError)
    )
    def call_api(self, api_func: Callable, *args, **kwargs):
        """Call API with retry logic"""
        try:
            return api_func(*args, **kwargs)
        except Exception as e:
            logger.error(f"{self.service_name} API call failed: {e}")
            raise
    
    def get_with_fallback(self, primary_func: Callable, fallback_func: Callable, *args, **kwargs):
        """Try primary API, fall back to secondary on failure"""
        try:
            return self.call_api(primary_func, *args, **kwargs)
        except Exception:
            logger.warning(f"{self.service_name} primary failed, trying fallback")
            try:
                return fallback_func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Both primary and fallback failed: {e}")
                return None