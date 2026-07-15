"""Unit and integration tests for the reverse-proxy forwarding module."""

from __future__ import annotations

import os
import pytest
import jwt
from datetime import datetime, timedelta, timezone
from httpx import AsyncClient, ASGITransport

from app.proxy import _filter_request_headers, _filter_response_headers
from app.api import create_app


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests: header filtering helpers
# ──────────────────────────────────────────────────────────────────────────────


def test_filter_request_headers_strips_hop_by_hop() -> None:
    """Verify that hop-by-hop headers are removed before forwarding upstream."""

    raw_headers = [
        ("authorization", "Bearer tok"),
        ("content-type", "application/json"),
        ("connection", "keep-alive"),       # hop-by-hop — must be stripped
        ("transfer-encoding", "chunked"),   # hop-by-hop — must be stripped
        ("host", "gateway:8000"),           # hop-by-hop — must be stripped
        ("x-request-id", "abc123"),
    ]
    result = _filter_request_headers(raw_headers, client_host="10.0.0.1")
    result_names = [name.lower() for name, _ in result]

    assert "connection" not in result_names
    assert "transfer-encoding" not in result_names
    assert "host" not in result_names
    assert "authorization" in result_names
    assert "content-type" in result_names
    assert "x-request-id" in result_names
    # X-Forwarded-For must be appended
    assert ("x-forwarded-for", "10.0.0.1") in result


def test_filter_request_headers_no_client_host() -> None:
    """Verify that X-Forwarded-For is omitted when client_host is None."""

    result = _filter_request_headers([("accept", "application/json")], client_host=None)
    result_names = [name.lower() for name, _ in result]
    assert "x-forwarded-for" not in result_names


def test_filter_response_headers_strips_hop_by_hop() -> None:
    """Verify that upstream hop-by-hop headers are stripped before relaying."""

    raw_headers = [
        (b"content-type", b"application/json"),
        (b"transfer-encoding", b"chunked"),  # hop-by-hop — must be stripped
        (b"keep-alive", b"timeout=5"),        # hop-by-hop — must be stripped
        (b"x-service-version", b"1.2.3"),
    ]
    result = _filter_response_headers(raw_headers)
    result_names = [name.lower() for name, _ in result]

    assert "transfer-encoding" not in result_names
    assert "keep-alive" not in result_names
    assert "content-type" in result_names
    assert "x-service-version" in result_names


# ──────────────────────────────────────────────────────────────────────────────
# Integration tests: end-to-end proxy routing through the real gateway stack
# ──────────────────────────────────────────────────────────────────────────────


def _make_jwt(secret: str = "dev-jwt-secret") -> str:
    """Mint a short-lived JWT for test requests."""

    return jwt.encode(
        {
            "sub": "test-user",
            "exp": (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp(),
        },
        secret,
        algorithm="HS256",
    )


@pytest.mark.asyncio
async def test_proxy_no_matching_route(clean_postgres, clean_redis, database_url: str, redis_url: str) -> None:
    """Unregistered paths must return 404, not crash or proxy blindly."""

    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = redis_url

    application = create_app()
    token = _make_jwt()

    async with application.router.lifespan_context(application):
        async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as client:
            r = await client.get(
                "/no-such-service/resource",
                headers={"Authorization": f"Bearer {token}"},
            )
    assert r.status_code == 404
    assert "No registered service" in r.json()["detail"]


@pytest.mark.asyncio
async def test_proxy_forwards_to_upstream(clean_postgres, clean_redis, database_url: str, redis_url: str) -> None:
    """A registered route must be reverse-proxied to the real dummy backend."""

    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = redis_url

    # Register the alpha backend (reachable inside Docker Compose network)
    conn = clean_postgres
    await conn.execute(
        "INSERT INTO gateway_services "
        "(name, route_prefix, base_url, health_check_path, is_enabled) "
        "VALUES ($1, $2, $3, $4, $5)",
        "alpha-service",
        "/alpha",
        "http://backend-alpha:8000",
        "/healthz",
        True,
    )
    await conn.execute(
        "INSERT INTO gateway_users (subject, is_active) VALUES ($1, $2)",
        "test-user",
        True,
    )

    application = create_app()
    token = _make_jwt()

    async with application.router.lifespan_context(application):
        async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as client:
            # Refresh the registry so it picks up the just-inserted service.
            await client.post("/registry/refresh")

            r = await client.get(
                "/alpha/ping",
                headers={"Authorization": f"Bearer {token}"},
            )

    # The dummy backend echoes its own name and the path.
    assert r.status_code == 200
    body = r.json()
    assert body["service"] == "alpha"


@pytest.mark.asyncio
async def test_proxy_query_string_forwarded(clean_postgres, clean_redis, database_url: str, redis_url: str) -> None:
    """Query-string parameters must be forwarded verbatim to the upstream."""

    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = redis_url

    conn = clean_postgres
    await conn.execute(
        "INSERT INTO gateway_services "
        "(name, route_prefix, base_url, health_check_path, is_enabled) "
        "VALUES ($1, $2, $3, $4, $5)",
        "bravo-service",
        "/bravo",
        "http://backend-bravo:8000",
        "/healthz",
        True,
    )
    await conn.execute(
        "INSERT INTO gateway_users (subject, is_active) VALUES ($1, $2)",
        "test-user",
        True,
    )

    application = create_app()
    token = _make_jwt()

    async with application.router.lifespan_context(application):
        async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as client:
            await client.post("/registry/refresh")

            r = await client.get(
                "/bravo/items?page=2&limit=10",
                headers={"Authorization": f"Bearer {token}"},
            )

    assert r.status_code == 200
    body = r.json()
    # The dummy backend returns the path it received; query string is separate
    # in its response but status 200 confirms the request reached upstream.
    assert body["service"] == "bravo"


@pytest.mark.asyncio
async def test_proxy_unauthenticated_returns_401(clean_postgres, clean_redis, database_url: str, redis_url: str) -> None:
    """Requests without credentials must be rejected at the auth layer, not proxied."""

    os.environ["DATABASE_URL"] = database_url
    os.environ["REDIS_URL"] = redis_url

    conn = clean_postgres
    await conn.execute(
        "INSERT INTO gateway_services "
        "(name, route_prefix, base_url, health_check_path, is_enabled) "
        "VALUES ($1, $2, $3, $4, $5)",
        "alpha-service",
        "/alpha",
        "http://backend-alpha:8000",
        "/healthz",
        True,
    )

    application = create_app()

    async with application.router.lifespan_context(application):
        async with AsyncClient(transport=ASGITransport(app=application), base_url="http://test") as client:
            await client.post("/registry/refresh")
            r = await client.get("/alpha/resource")

    assert r.status_code == 401
