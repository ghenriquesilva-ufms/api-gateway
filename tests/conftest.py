"""Shared test fixtures and configuration for gateway tests."""

from __future__ import annotations

import os
import pytest
import pytest_asyncio
import redis.asyncio as redis
import asyncpg


@pytest.fixture(scope="session")
def redis_url() -> str:
    """Return the Redis connection string for testing.

    Falls back to a testcontainer or default local Redis if not running
    inside Docker Compose.
    """

    env_url = os.getenv("REDIS_URL")
    if env_url:
        # Use DB 1 for testing to isolate from development cache
        if env_url.endswith("/0"):
            return env_url[:-1] + "1"
        return env_url

    try:
        from testcontainers.redis import RedisContainer
        container = RedisContainer("redis:7-alpine")
        container.start()
        # Register a teardown
        url = container.get_connection_url()
        return url
    except Exception:
        return "redis://localhost:6379/1"


@pytest.fixture(scope="session")
def database_url() -> str:
    """Return the Postgres connection string for testing.

    Falls back to a testcontainer or default local Postgres if not running
    inside Docker Compose.
    """

    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    try:
        from testcontainers.postgres import PostgresContainer
        container = PostgresContainer("postgres:16-alpine", username="gateway", password="gateway", dbname="gateway")
        container.start()
        return container.get_connection_url()
    except Exception:
        return "postgresql://gateway:gateway@localhost:5432/gateway"


@pytest_asyncio.fixture
async def clean_postgres(database_url: str):
    """Truncate Postgres gateway tables before and after each test."""

    conn = await asyncpg.connect(database_url)
    try:
        # Ensure schema tables exist (for testcontainers)
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gateway_users (
                id SERIAL PRIMARY KEY,
                subject VARCHAR(255) UNIQUE NOT NULL,
                is_active BOOLEAN DEFAULT TRUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gateway_api_keys (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES gateway_users(id) ON DELETE CASCADE NOT NULL,
                key_name VARCHAR(255) NOT NULL,
                key_hash VARCHAR(64) UNIQUE NOT NULL,
                is_active BOOLEAN DEFAULT TRUE NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
            );
            CREATE TABLE IF NOT EXISTS gateway_services (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                route_prefix VARCHAR(255) UNIQUE NOT NULL,
                base_url VARCHAR(255) NOT NULL,
                health_check_path VARCHAR(255) NOT NULL,
                is_enabled BOOLEAN DEFAULT TRUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
            );
            """
        )
        await conn.execute("TRUNCATE gateway_api_keys, gateway_users, gateway_services CASCADE")
        yield conn
    finally:
        await conn.execute("TRUNCATE gateway_api_keys, gateway_users, gateway_services CASCADE")
        await conn.close()


@pytest_asyncio.fixture
async def clean_redis(redis_url: str):
    """Flush the active Redis database before and after each test."""

    client = redis.from_url(redis_url, decode_responses=True)
    try:
        await client.flushdb()
        yield client
    finally:
        await client.flushdb()
        await client.aclose()
