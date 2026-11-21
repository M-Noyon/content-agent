import time

import pytest

from app.retry import RateLimiter, RetryableError, retry_async


async def test_retries_then_succeeds():
    calls = {"n": 0}

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RetryableError("429 Too Many Requests", retry_after=0.01)
        return "ok"

    assert await retry_async(flaky, attempts=5, base_delay=0.01) == "ok"
    assert calls["n"] == 3


async def test_gives_up_after_max_attempts():
    async def always_fails():
        raise RetryableError("503", retry_after=0.01)

    with pytest.raises(RetryableError):
        await retry_async(always_fails, attempts=3, base_delay=0.01)


async def test_non_retryable_error_raises_immediately():
    calls = {"n": 0}

    async def bad_request():
        calls["n"] += 1
        raise ValueError("400 bad request")

    with pytest.raises(ValueError):
        await retry_async(bad_request, attempts=5, base_delay=0.01)
    assert calls["n"] == 1


async def test_rate_limiter_enforces_window():
    limiter = RateLimiter(max_calls=2, period=0.3)
    start = time.monotonic()
    for _ in range(3):
        await limiter.acquire()
    assert time.monotonic() - start >= 0.28
