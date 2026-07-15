"""Unit and integration tests for the gateway circuit breaker module."""

from __future__ import annotations

import os
import asyncio
import pytest
import jwt
from datetime import datetime, timedelta, timezone
from httpx import AsyncClient, ASGITransport

from app.circuit_breaker import CircuitBreaker, CircuitState
from app.api import create_app


@pytest.mark.asyncio
async def test_circuit_breaker_transitions(clean_redis, redis_url: str) -> None:
    """Test all circuit breaker state transitions (CLOSED, OPEN, HALF_OPEN)."""

    # Low thresholds and timeouts to keep tests fast
    breaker = CircuitBreaker(
        redis_url=redis_url,
        service_name="test-service",
        failure_threshold=3,
        recovery_timeout_seconds=0.1,
        failure_window_seconds=1.0,
        trial_timeout_seconds=0.5,
    )
    await breaker.startup()

    try:
        # 1. Starts CLOSED
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.CLOSED
        assert snapshot.failure_count == 0
        assert await breaker.allow_request() is True

        # 2. Record 2 failures - remains CLOSED
        await breaker.record_failure()
        await breaker.record_failure()
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.CLOSED
        assert snapshot.failure_count == 2
        assert await breaker.allow_request() is True

        # 3. Record 3rd failure - trips to OPEN
        await breaker.record_failure()
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.OPEN
        assert await breaker.allow_request() is False

        # 4. Wait for recovery timeout
        await asyncio.sleep(0.12)

        # 5. Next request checks allow_request - moves to HALF_OPEN (returns True for trial)
        assert await breaker.allow_request() is True
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.HALF_OPEN

        # 6. While HALF_OPEN, concurrent requests are blocked
        assert await breaker.allow_request() is False

        # 7. Record trial failure - transitions immediately back to OPEN
        await breaker.record_failure()
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.OPEN
        assert await breaker.allow_request() is False

        # 8. Wait cooldown again
        await asyncio.sleep(0.12)

        # 9. Trial request allowed, transitions to HALF_OPEN
        assert await breaker.allow_request() is True

        # 10. Record trial success - transitions back to CLOSED
        await breaker.record_success()
        snapshot = await breaker.snapshot()
        assert snapshot.state == CircuitState.CLOSED
        assert snapshot.failure_count == 0
        assert await breaker.allow_request() is True

    finally:
        await breaker.shutdown()


@pytest.mark.asyncio
async def test_integration_circuit_breaker(clean_postgres, clean_redis, database_url: str, redis_url: str) -> None:
    """End-to-end integration test for the circuit breaker middleware and API pipeline."""

    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = redis_url
    os.environ["CIRCUIT_BREAKER_FAILURE_THRESHOLD"] = "3"
    os.environ["CIRCUIT_BREAKER_RECOVERY_TIMEOUT"] = "0.2"
    os.environ["CIRCUIT_BREAKER_FAILURE_WINDOW"] = "1.0"

    app = create_app()

    # Seed Postgres with a service configuration
    conn = clean_postgres
    await conn.execute(
        "INSERT INTO gateway_services (name, route_prefix, base_url, health_check_path, is_enabled) "
        "VALUES ($1, $2, $3, $4, $5)",
        "alpha-service",
        "/alpha",
        "http://backend-alpha:8000",
        "/healthz",
        True,
    )

    # Seed Postgres with an active user for JWT authentication
    await conn.execute(
        "INSERT INTO gateway_users (id, subject, is_active) VALUES ($1, $2, $3)",
        1,
        "test-user",
        True,
    )

    # Create a valid JWT token
    token = jwt.encode(
        {"sub": "test-user", "exp": (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp()},
        "dev-jwt-secret",
        algorithm="HS256",
    )

    # Use app lifespan context to initialize pools (database, redis, registry)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            # Force a service registry refresh to pick up the seeded service
            ref_resp = await client.post("/registry/refresh")
            assert ref_resp.status_code == 200

            headers = {"Authorization": f"Bearer {token}"}

            # 1. Trigger 2 failures (simulated via headers)
            r = await client.get("/alpha/test", headers={**headers, "X-Test-Fail": "true"})
            assert r.status_code == 500

            r = await client.get("/alpha/test", headers={**headers, "X-Test-Fail": "true"})
            assert r.status_code == 500

            # Gateway remains closed (requires 3 failures)
            r = await client.get("/alpha/test", headers=headers)
            assert r.status_code == 200

            # 2. Trigger 3rd failure to trip the circuit
            r = await client.get("/alpha/test", headers={**headers, "X-Test-Fail": "true"})
            assert r.status_code == 500

            # 3. Subsequent request must fast-fail with 503 Service Unavailable
            r = await client.get("/alpha/test", headers=headers)
            assert r.status_code == 503
            assert "Retry-After" in r.headers

            # 4. Wait for recovery timeout (200ms)
            await asyncio.sleep(0.22)

            # 5. First request should act as a trial and be allowed to proceed
            r = await client.get("/alpha/test", headers=headers)
            assert r.status_code == 200

            # 6. Circuit breaker should recover to CLOSED, subsequent requests succeed
            r = await client.get("/alpha/test", headers=headers)
            assert r.status_code == 200
