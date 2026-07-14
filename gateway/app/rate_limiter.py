"""Redis-backed token bucket rate limiting primitives for the gateway.

This module keeps the gateway stateless by storing all shared request budgets
in Redis and updating them with an atomic Lua script.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass

import redis.asyncio as redis


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitDecision:
    """Describe the result of a rate limit check for a single request."""

    allowed: bool
    remaining_tokens: float
    retry_after_seconds: float | None


def _json_log(event: str, **fields: object) -> None:
    """Emit structured JSON logs for rate-limit decisions."""

    LOGGER.info(json.dumps({"event": event, **fields}, default=str))


def _bucket_key(subject: str, scope: str) -> str:
    """Derive a stable Redis key for a rate-limit bucket.

    Hashing keeps caller-controlled identifiers compact and avoids exposing raw
    principal data in the Redis keyspace.
    """

    digest = hashlib.sha256(f"{scope}:{subject}".encode("utf-8")).hexdigest()
    return f"ratelimit:{scope}:{digest}"


class TokenBucketRateLimiter:
    """Coordinate shared token-bucket state stored in Redis.

    The limiter uses a Redis Lua script so the refill, check, and decrement are
    evaluated atomically even when many gateway replicas race on the same key.
    """

    LUA_SCRIPT = """
    local key = KEYS[1]
    local capacity = tonumber(ARGV[1])
    local refill_per_second = tonumber(ARGV[2])
    local cost = tonumber(ARGV[3])

    local now = redis.call('TIME')
    local now_ms = (tonumber(now[1]) * 1000) + math.floor(tonumber(now[2]) / 1000)

    local stored_tokens = redis.call('HGET', key, 'tokens')
    local stored_ts = redis.call('HGET', key, 'ts_ms')

    local tokens = capacity
    local last_refill_ms = now_ms

    if stored_tokens then
        tokens = tonumber(stored_tokens) or capacity
    end

    if stored_ts then
        last_refill_ms = tonumber(stored_ts) or now_ms
    end

    local elapsed_ms = math.max(0, now_ms - last_refill_ms)
    local refill_amount = elapsed_ms * (refill_per_second / 1000.0)
    tokens = math.min(capacity, tokens + refill_amount)

    local allowed = 0
    local retry_after_seconds = 0

    if tokens >= cost then
        allowed = 1
        tokens = tokens - cost
    else
        local missing_tokens = cost - tokens
        if refill_per_second > 0 then
            retry_after_seconds = math.ceil(missing_tokens / refill_per_second)
        else
            retry_after_seconds = 60
        end
    end

    redis.call('HSET', key, 'tokens', tokens, 'ts_ms', now_ms)

    local ttl_seconds = math.max(60, math.ceil((capacity / math.max(refill_per_second, 0.001)) * 2))
    redis.call('EXPIRE', key, ttl_seconds)

    return { allowed, tokens, retry_after_seconds }
    """

    def __init__(
        self,
        redis_url: str,
        *,
        capacity: float = 10.0,
        refill_rate_per_second: float = 5.0,
        request_cost: float = 1.0,
    ) -> None:
        """Store Redis connectivity and token bucket policy settings."""

        self.redis_url = redis_url
        self.capacity = capacity
        self.refill_rate_per_second = refill_rate_per_second
        self.request_cost = request_cost
        self._client: redis.Redis | None = None

    async def startup(self) -> None:
        """Create the Redis client used to evaluate token bucket decisions."""

        self._client = redis.from_url(self.redis_url, decode_responses=True)

    async def shutdown(self) -> None:
        """Close the Redis client used by the limiter."""

        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # === STUDY-NOTE START ===
    # WHAT THIS DOES: Runs the entire token-bucket check and decrement inside a
    # Redis Lua script so the operation is atomic across all gateway instances.
    # WHY THIS APPROACH: A read-then-write flow in application code races when two
    # replicas observe the same token count and both decide to allow the request.
    # COMMON WRONG IMPLEMENTATION: GET the bucket, compute in Python, then HSET
    # the new state, which can overspend tokens under concurrent access.
    # IF YOU'RE STUCK: Keep refill math, allowance, and write-back inside one Lua
    # script and let Redis provide the single serialization point.
    # === STUDY-NOTE END ===
    async def allow(self, *, subject: str, scope: str) -> RateLimitDecision:
        """Check whether a request should consume a token for the given scope."""

        if self._client is None:
            raise RuntimeError("TokenBucketRateLimiter is not initialized.")

        key = _bucket_key(subject=subject, scope=scope)
        result = await self._client.eval(
            self.LUA_SCRIPT,
            1,
            key,
            self.capacity,
            self.refill_rate_per_second,
            self.request_cost,
        )

        allowed = bool(int(result[0]))
        remaining_tokens = float(result[1])
        retry_after_seconds = float(result[2]) if not allowed else None

        decision = RateLimitDecision(
            allowed=allowed,
            remaining_tokens=remaining_tokens,
            retry_after_seconds=retry_after_seconds,
        )
        _json_log(
            "rate_limit_decision",
            subject=subject,
            scope=scope,
            allowed=decision.allowed,
            remaining_tokens=decision.remaining_tokens,
            retry_after_seconds=decision.retry_after_seconds,
        )
        return decision
