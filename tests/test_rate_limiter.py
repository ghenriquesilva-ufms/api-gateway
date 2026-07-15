"""Unit tests for the token bucket rate limiter module."""

from __future__ import annotations

import asyncio
import pytest

from app.rate_limiter import TokenBucketRateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_allow(clean_redis, redis_url: str) -> None:
    """Test standard rate limit allowance, exhaustion, and retry metadata."""

    limiter = TokenBucketRateLimiter(
        redis_url=redis_url,
        capacity=5.0,
        refill_rate_per_second=1.0,
    )
    await limiter.startup()

    try:
        # First 5 requests should be allowed
        for _ in range(5):
            decision = await limiter.allow(subject="user1", scope="test")
            assert decision.allowed is True
            assert decision.remaining_tokens >= 0.0

        # 6th request should be blocked
        decision = await limiter.allow(subject="user1", scope="test")
        assert decision.allowed is False
        assert decision.retry_after_seconds is not None
        assert decision.retry_after_seconds >= 1.0

    finally:
        await limiter.shutdown()


@pytest.mark.asyncio
async def test_rate_limiter_concurrency(clean_redis, redis_url: str) -> None:
    """Test that concurrent rate limit requests evaluate atomically without double-spending."""

    limiter = TokenBucketRateLimiter(
        redis_url=redis_url,
        capacity=5.0,
        refill_rate_per_second=0.0,  # No refill during test to simplify counting
    )
    await limiter.startup()

    try:
        # Spawn 20 concurrent requests simultaneously
        tasks = [limiter.allow(subject="user_concurrent", scope="test") for _ in range(20)]
        decisions = await asyncio.gather(*tasks)

        allowed_count = sum(1 for d in decisions if d.allowed)
        blocked_count = sum(1 for d in decisions if not d.allowed)

        # Atomic check-and-decrement must guarantee exactly capacity is allowed
        assert allowed_count == 5
        assert blocked_count == 15

    finally:
        await limiter.shutdown()
