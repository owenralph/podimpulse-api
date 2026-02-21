import time
import logging
from typing import Callable, Any, Type, Tuple

def retry_with_backoff(
    func: Callable,
    exceptions: Tuple[Type[BaseException], ...],
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    logger: logging.Logger = logging,
    operation_name: str | None = None
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
        op_name = operation_name or getattr(func, "__name__", "unknown")
        logger.debug(
            f"Starting retry_with_backoff for {op_name} with max_attempts={max_attempts}."
        )
        start = time.perf_counter()
        delay = initial_delay
        for attempt in range(1, max_attempts + 1):
            try:
                result = func(*args, **kwargs)
                elapsed_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    f"[metric] retry.success operation={op_name} attempts={attempt} elapsed_ms={elapsed_ms:.2f}"
                )
                return result
            except exceptions as e:
                logger.warning(
                    f"[metric] retry.attempt_failed operation={op_name} attempt={attempt} "
                    f"max_attempts={max_attempts} error={e}"
                )
                if attempt == max_attempts:
                    elapsed_ms = (time.perf_counter() - start) * 1000
                    logger.error(
                        f"[metric] retry.exhausted operation={op_name} attempts={attempt} "
                        f"elapsed_ms={elapsed_ms:.2f} error={e}"
                    )
                    raise
                time.sleep(delay)
                delay *= backoff_factor
    return wrapper
