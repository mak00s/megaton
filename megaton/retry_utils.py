"""Shared retry helpers (exponential backoff).

Megaton has multiple Google API entrypoints (GA4, Search Console, Sheets).
Keep retry behavior consistent by centralizing the core logic here.
"""

from __future__ import annotations

import random
import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def expo_retry(
    func: Callable[[], T],
    *,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    is_retryable: Optional[Callable[[BaseException], bool]] = None,
    on_retry: Optional[Callable[[int, int, float, BaseException], None]] = None,
    sleep: Callable[[float], None] = time.sleep,
    jitter: float = 0.0,
    max_wait: Optional[float] = None,
    max_elapsed: Optional[float] = None,
    now: Callable[[], float] = time.monotonic,
) -> T:
    """Run func with exponential backoff.

    Args:
        func: Zero-arg callable to execute.
        max_retries: Number of attempts (>= 1).
        backoff_factor: Wait multiplier. Wait = backoff_factor * (2 ** attempt_index).
        exceptions: Exception types to catch and potentially retry.
        is_retryable: Optional predicate to decide whether an exception is retryable.
        on_retry: Optional callback called before sleeping.
            Signature: (attempt_no, max_retries, wait_seconds, exception)
            attempt_no is 1-based and refers to the attempt that just failed.
        sleep: Sleep function (injectable for tests).
        jitter: Randomize wait by multiplying with U(1-jitter, 1+jitter). Range: [0, 1).
        max_wait: Cap a single wait duration (seconds). None means no cap.
        max_elapsed: Cap total elapsed time (seconds). None means no cap.
        now: Clock function for elapsed time tracking (injectable for tests).

    Returns:
        func() return value.

    Raises:
        The last exception if retries are exhausted or non-retryable.
    """
    try:
        max_retries = int(max_retries)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("max_retries must be an int") from exc
    if max_retries < 1:
        raise ValueError("max_retries must be >= 1")

    try:
        backoff_factor = float(backoff_factor)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("backoff_factor must be a float") from exc
    if backoff_factor < 0:
        raise ValueError("backoff_factor must be >= 0")

    try:
        jitter = float(jitter)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("jitter must be a float") from exc
    if not (0.0 <= jitter < 1.0):
        raise ValueError("jitter must be in [0, 1)")

    if max_wait is not None:
        try:
            max_wait = float(max_wait)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("max_wait must be a float") from exc
        if max_wait < 0:
            raise ValueError("max_wait must be >= 0")

    if max_elapsed is not None:
        try:
            max_elapsed = float(max_elapsed)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("max_elapsed must be a float") from exc
        if max_elapsed < 0:
            raise ValueError("max_elapsed must be >= 0")

    start = now()

    for attempt_index in range(max_retries):
        try:
            return func()
        except exceptions as exc:
            if is_retryable is not None and not is_retryable(exc):
                raise
            attempt_no = attempt_index + 1
            if attempt_no >= max_retries:
                raise
            if max_elapsed is not None and (now() - start) >= max_elapsed:
                # Preserve the original exception type/cause (do not raise a new one).
                raise
            wait = backoff_factor * (2**attempt_index)
            if jitter:
                wait *= random.uniform(1.0 - jitter, 1.0 + jitter)
            if max_wait is not None:
                wait = min(wait, max_wait)
            if max_elapsed is not None:
                remaining = max_elapsed - (now() - start)
                if remaining <= 0:
                    raise
                wait = min(wait, remaining)
            if on_retry is not None:
                on_retry(attempt_no, max_retries, wait, exc)
            sleep(wait)

    # Unreachable (loop always returns or raises).
    raise RuntimeError("expo_retry reached an unexpected state")
