import time
import logging
from typing import Callable, Any, Type, Tuple

def retry_with_backoff(
    func: Callable,
    exceptions: Tuple[Type[BaseException], ...],
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    logger: logging.Logger = logging
) -> Callable:
    """
    Retry a function with exponential backoff on specified exceptions.

    Args:
        func (Callable): The function to retry.
        exceptions (Tuple[Type[BaseException], ...]): Exceptions to catch and retry on.
        max_attempts (int): Maximum number of attempts.
        initial_delay (float): Initial delay in seconds.
        backoff_factor (float): Factor to multiply delay after each failure.
        logger (logging.Logger): Logger for warnings/errors.

    Returns:
        Callable: The wrapped function with retry logic.
    """
    def wrapper(*args, **kwargs) -> Any:
        logger.debug(f"Starting retry_with_backoff for {func.__name__} with max_attempts={max_attempts}.")
        delay = initial_delay
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logger.warning(f"Attempt {attempt} failed: {e}")
                if attempt == max_attempts:
                    logger.error(f"Max retry attempts reached. Last error: {e}")
                    raise
                time.sleep(delay)
                delay *= backoff_factor
    return wrapper
