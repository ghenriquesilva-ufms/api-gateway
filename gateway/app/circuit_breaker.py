"""Circuit breaker state management for downstream services."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum

import redis.asyncio as redis

LOGGER = logging.getLogger(__name__)


def _json_log(event: str, **fields: object) -> None:
    """Emit structured JSON logs for circuit breaker decisions and transitions."""

    LOGGER.info(json.dumps({"event": event, **fields}, default=str))


def _cb_state_key(service_name: str) -> str:
    """Generate the Redis key for the circuit breaker's hash state."""

    return f"cb:{service_name}:state_hash"


def _cb_failures_key(service_name: str) -> str:
    """Generate the Redis key for the circuit breaker's failure ZSET."""

    return f"cb:{service_name}:failures_zset"


class CircuitState(str, Enum):
    """Represent the lifecycle states of a downstream circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class CircuitSnapshot:
    """Capture the current state of a service circuit breaker."""

    state: CircuitState
    failure_count: int
    last_failure_unix_ms: int | None


class CircuitBreaker:
    """Coordinate circuit-breaker decisions for a single downstream service.

    This class provides a shared circuit-breaker state machine (CLOSED, OPEN,
    HALF-OPEN) stored in Redis to coordinate failure-isolation across all
    active API gateway replicas.
    """

    ALLOW_SCRIPT = """
    local state_key = KEYS[1]
    local cooldown_ms = tonumber(ARGV[1])
    local trial_timeout_ms = tonumber(ARGV[2])

    local state = redis.call('HGET', state_key, 'state') or 'closed'
    local open_ts = tonumber(redis.call('HGET', state_key, 'open_ts')) or 0
    local trial_start_ts = tonumber(redis.call('HGET', state_key, 'trial_start_ts')) or 0

    local time = redis.call('TIME')
    local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)

    if state == 'closed' then
        return {1, 'closed', 'closed'}
    elseif state == 'open' then
        if now_ms - open_ts >= cooldown_ms then
            redis.call('HSET', state_key, 'state', 'half_open', 'trial_start_ts', now_ms)
            return {1, 'half_open', 'open'}
        else
            return {0, 'open', 'open'}
        end
    elseif state == 'half_open' then
        if now_ms - trial_start_ts >= trial_timeout_ms then
            redis.call('HSET', state_key, 'trial_start_ts', now_ms)
            return {1, 'half_open', 'half_open'}
        else
            return {0, 'half_open', 'half_open'}
        end
    end
    return {0, 'unknown', 'unknown'}
    """

    SUCCESS_SCRIPT = """
    local state_key = KEYS[1]
    local failures_key = KEYS[2]

    local state = redis.call('HGET', state_key, 'state') or 'closed'

    if state == 'half_open' then
        redis.call('HSET', state_key, 'state', 'closed')
        redis.call('HDEL', state_key, 'open_ts', 'trial_start_ts', 'failure_counter')
        redis.call('DEL', failures_key)
        return {'closed', 'half_open'}
    elseif state == 'closed' then
        return {'closed', 'closed'}
    end
    return {state, state}
    """

    FAILURE_SCRIPT = """
    local state_key = KEYS[1]
    local failures_key = KEYS[2]
    local failure_threshold = tonumber(ARGV[1])
    local failure_window_ms = tonumber(ARGV[2])

    local time = redis.call('TIME')
    local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)

    local state = redis.call('HGET', state_key, 'state') or 'closed'

    if state == 'open' then
        return {'open', 'open'}
    elseif state == 'half_open' then
        redis.call('HSET', state_key, 'state', 'open', 'open_ts', now_ms)
        redis.call('HDEL', state_key, 'trial_start_ts', 'failure_counter')
        return {'open', 'half_open'}
    elseif state == 'closed' then
        local f_id = redis.call('HINCRBY', state_key, 'failure_counter', 1)
        local member = now_ms .. ':' .. f_id
        redis.call('ZADD', failures_key, now_ms, member)
        
        local min_ts = now_ms - failure_window_ms
        redis.call('ZREMRANGEBYSCORE', failures_key, '-inf', min_ts)
        local count = redis.call('ZCARD', failures_key)
        
        redis.call('HSET', state_key, 'last_failure_ts', now_ms)

        if count >= failure_threshold then
            redis.call('HSET', state_key, 'state', 'open', 'open_ts', now_ms)
            return {'open', 'closed'}
        else
            return {'closed', 'closed'}
        end
    end
    return {'unknown', 'unknown'}
    """

    SNAPSHOT_SCRIPT = """
    local state_key = KEYS[1]
    local failures_key = KEYS[2]
    local failure_window_ms = tonumber(ARGV[1])

    local time = redis.call('TIME')
    local now_ms = (tonumber(time[1]) * 1000) + math.floor(tonumber(time[2]) / 1000)

    local state = redis.call('HGET', state_key, 'state') or 'closed'
    local last_failure_ts = redis.call('HGET', state_key, 'last_failure_ts')

    local min_ts = now_ms - failure_window_ms
    redis.call('ZREMRANGEBYSCORE', failures_key, '-inf', min_ts)
    local count = redis.call('ZCARD', failures_key)

    return { state, count, last_failure_ts }
    """

    def __init__(
        self,
        redis_url: str,
        service_name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        failure_window_seconds: float = 60.0,
        trial_timeout_seconds: float = 10.0,
    ) -> None:
        """Initialize the shared state location and the downstream service configuration.

        Configurable options are exposed to adapt failure detection and recovery
        times for different downstream service dependencies.
        """

        self.redis_url = redis_url
        self.service_name = service_name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self.failure_window_seconds = failure_window_seconds
        self.trial_timeout_seconds = trial_timeout_seconds
        self._client: redis.Redis | None = None

        self._state_key = _cb_state_key(service_name)
        self._failures_key = _cb_failures_key(service_name)

    async def startup(self) -> None:
        """Initialize the Redis client for state coordination."""

        self._client = redis.from_url(self.redis_url, decode_responses=True)

    async def shutdown(self) -> None:
        """Close the Redis client gracefully."""

        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # === STUDY-NOTE START ===
    # WHAT THIS DOES: Evaluates circuit-breaker state transitions and trial request
    # routing atomically across gateway instances using Redis Lua scripts.
    # WHY THIS APPROACH: Shared state in Redis ensures that multiple API gateway
    # instances behave as a single cohesive unit, instantly sharing downstream health.
    # COMMON WRONG IMPLEMENTATION: Checking failure counts without a rolling time
    # window (e.g., global failure counter). A service with 1 failure per hour would
    # eventually trip the circuit if the threshold is 5, despite being healthy.
    # IF YOU'RE STUCK: Use a Redis Sorted Set (ZSET) to store failure timestamps.
    # Prune elements older than the sliding window before counting current failures.
    # === STUDY-NOTE END ===
    async def allow_request(self) -> bool:
        """Check if a request is allowed to proceed to the service.

        Transitions to HALF-OPEN atomically if the cooldown period has expired
        to allow a single trial request.
        """

        if self._client is None:
            raise RuntimeError("CircuitBreaker is not initialized.")

        cooldown_ms = int(self.recovery_timeout_seconds * 1000)
        trial_timeout_ms = int(self.trial_timeout_seconds * 1000)

        result = await self._client.eval(
            self.ALLOW_SCRIPT,
            1,
            self._state_key,
            cooldown_ms,
            trial_timeout_ms,
        )

        allowed = bool(int(result[0]))
        new_state = str(result[1])
        old_state = str(result[2])

        if new_state != old_state:
            _json_log(
                "circuit_breaker_transition",
                service_name=self.service_name,
                old_state=old_state,
                new_state=new_state,
                reason="cooldown_expired_or_trial_retry",
            )

        _json_log(
            "circuit_breaker_decision",
            service_name=self.service_name,
            allowed=allowed,
            state=new_state,
        )

        return allowed

    async def record_success(self) -> None:
        """Record a successful request to the service.

        If in HALF-OPEN state, this successfully closes the circuit and clears
        all failure records.
        """

        if self._client is None:
            raise RuntimeError("CircuitBreaker is not initialized.")

        result = await self._client.eval(
            self.SUCCESS_SCRIPT,
            2,
            self._state_key,
            self._failures_key,
        )

        new_state = str(result[0])
        old_state = str(result[1])

        if new_state != old_state:
            _json_log(
                "circuit_breaker_transition",
                service_name=self.service_name,
                old_state=old_state,
                new_state=new_state,
                reason="trial_request_succeeded",
            )

    async def record_failure(self) -> None:
        """Record a failed request to the service.

        Increments the failure counter (in a sliding time window) or transitions
        back to OPEN if a trial request fails.
        """

        if self._client is None:
            raise RuntimeError("CircuitBreaker is not initialized.")

        threshold = self.failure_threshold
        window_ms = int(self.failure_window_seconds * 1000)

        result = await self._client.eval(
            self.FAILURE_SCRIPT,
            2,
            self._state_key,
            self._failures_key,
            threshold,
            window_ms,
        )

        new_state = str(result[0])
        old_state = str(result[1])

        if new_state != old_state:
            reason = "failure_threshold_exceeded" if old_state == "closed" else "trial_request_failed"
            _json_log(
                "circuit_breaker_transition",
                service_name=self.service_name,
                old_state=old_state,
                new_state=new_state,
                reason=reason,
            )

    async def snapshot(self) -> CircuitSnapshot:
        """Read the current breaker state for the configured service.

        Prunes expired failure entries and returns a point-in-time state check.
        """

        if self._client is None:
            raise RuntimeError("CircuitBreaker is not initialized.")

        window_ms = int(self.failure_window_seconds * 1000)

        result = await self._client.eval(
            self.SNAPSHOT_SCRIPT,
            2,
            self._state_key,
            self._failures_key,
            window_ms,
        )

        state_str = str(result[0])
        failure_count = int(result[1])
        last_failure_unix_ms = int(result[2]) if result[2] is not None else None

        return CircuitSnapshot(
            state=CircuitState(state_str),
            failure_count=failure_count,
            last_failure_unix_ms=last_failure_unix_ms,
        )
