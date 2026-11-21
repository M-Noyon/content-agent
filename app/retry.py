"""Fault tolerance: async retry with exponential backoff + jitter, and a rate limiter."""
import asyncio
import logging
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import TypeVar

log = logging.getLogger(__name__)
T = TypeVar("T")


class RetryableError(Exception):
    """Raised for transient failures (429, 5xx, network). retry_after = server hint in seconds."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


async def retry_async(
    fn: Callable[[], Awaitable[T]],
    *,
    attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    label: str = "call",
) -> T:
    """Call fn(); on RetryableError wait (server hint or exponential backoff + jitter) and retry."""
    for attempt in range(1, attempts + 1):
        try:
            return await fn()
        except RetryableError as exc:
            if attempt == attempts:
                log.error("%s failed after %d attempts: %s", label, attempts, exc)
                raise
            if exc.retry_after is not None:
                delay = float(exc.retry_after)
            else:
                delay = min(max_delay, base_delay * 2 ** (attempt - 1))
                delay += random.uniform(0, delay * 0.25)  # jitter avoids thundering herd
            log.warning("%s attempt %d/%d failed (%s); retrying in %.1fs",
                        label, attempt, attempts, exc, delay)
            await asyncio.sleep(delay)
    raise RuntimeError("unreachable")


class RateLimiter:
    """Sliding-window limiter: at most max_calls per period seconds (FIFO for waiters)."""

    def __init__(self, max_calls: int, period: float = 60.0):
        self.max_calls = max_calls
        self.period = period
        self._calls: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                while self._calls and now - self._calls[0] >= self.period:
                    self._calls.popleft()
                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return
                await asyncio.sleep(self.period - (now - self._calls[0]))
