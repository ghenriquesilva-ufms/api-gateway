"""Circuit breaker state management for downstream services."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


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
    """Coordinate circuit-breaker decisions for a single downstream service."""

    def __init__(self, redis_url: str, service_name: str) -> None:
        """Store the shared state location and the downstream service name."""

        self.redis_url = redis_url
        self.service_name = service_name

    async def snapshot(self) -> CircuitSnapshot:
        """Read the current breaker state for the configured service."""

        raise NotImplementedError("Circuit breaker state reads will be implemented later.")
