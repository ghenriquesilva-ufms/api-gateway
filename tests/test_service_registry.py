"""Unit tests for the service registry module."""

from __future__ import annotations

import pytest

from app.service_registry import ServiceRegistry


@pytest.mark.asyncio
async def test_service_registry_resolution(clean_postgres, database_url: str) -> None:
    """Test loading services from DB, path normalization, and prefix route matching."""

    registry = ServiceRegistry(
        database_url=database_url,
        refresh_interval_seconds=60.0,  # Long refresh interval to prevent background task noise
    )

    # Seed the database
    conn = clean_postgres
    await conn.execute(
        "INSERT INTO gateway_services (name, route_prefix, base_url, health_check_path, is_enabled) "
        "VALUES ($1, $2, $3, $4, $5)",
        "alpha-service",
        "alpha",
        "http://backend-alpha:8000",
        "/healthz",
        True,
    )

    await registry.startup()

    try:
        # 1. Test exact prefix matching
        route = registry.get_route_for_path("/alpha")
        assert route is not None
        assert route.name == "alpha-service"
        assert route.base_url == "http://backend-alpha:8000"

        # 2. Test sub-path prefix matching
        route = registry.get_route_for_path("/alpha/users/profile")
        assert route is not None
        assert route.name == "alpha-service"

        # 3. Test non-matching route prefix
        route = registry.get_route_for_path("/beta")
        assert route is None

    finally:
        await registry.shutdown()
