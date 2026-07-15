"""Unit tests for gateway authentication module."""

from __future__ import annotations

import pytest
import jwt
from datetime import datetime, timedelta, timezone

from app.auth import GatewayAuthenticator, AuthenticationError, _hash_api_key


class DummyRequest:
    """Mock Request class to test authorization headers."""

    def __init__(self, headers: dict[str, str]) -> None:
        self.headers = headers


@pytest.mark.asyncio
async def test_jwt_validation_success() -> None:
    """Test validation of a valid JWT token with claims."""

    secret = "test-secret"
    authenticator = GatewayAuthenticator(
        database_url="",
        jwt_secret=secret,
        jwt_audience="test-aud",
        jwt_issuer="test-iss",
    )

    payload = {
        "sub": "user123",
        "aud": "test-aud",
        "iss": "test-iss",
        "scope": "read write",
        "exp": (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp(),
    }
    token = jwt.encode(payload, secret, algorithm="HS256")

    req = DummyRequest({"Authorization": f"Bearer {token}"})
    context = await authenticator.authenticate_request(req)  # type: ignore

    assert context.principal_id == "user123"
    assert context.auth_type == "jwt"
    assert "read" in context.scopes
    assert "write" in context.scopes


@pytest.mark.asyncio
async def test_jwt_validation_expired() -> None:
    """Test validation of an expired JWT token."""

    secret = "test-secret"
    authenticator = GatewayAuthenticator(
        database_url="",
        jwt_secret=secret,
    )

    payload = {
        "sub": "user123",
        "exp": (datetime.now(timezone.utc) - timedelta(minutes=5)).timestamp(),
    }
    token = jwt.encode(payload, secret, algorithm="HS256")

    req = DummyRequest({"Authorization": f"Bearer {token}"})
    with pytest.raises(AuthenticationError) as exc:
        await authenticator.authenticate_request(req)  # type: ignore
    assert "expired" in str(exc.value)


@pytest.mark.asyncio
async def test_jwt_validation_clock_skew() -> None:
    """Test validation of a slightly expired token within the clock skew leeway."""

    secret = "test-secret"
    authenticator = GatewayAuthenticator(
        database_url="",
        jwt_secret=secret,
        jwt_clock_skew_seconds=30,
    )

    # Token expired 10 seconds ago, should be accepted with 30s leeway
    payload = {
        "sub": "user123",
        "exp": (datetime.now(timezone.utc) - timedelta(seconds=10)).timestamp(),
    }
    token = jwt.encode(payload, secret, algorithm="HS256")

    req = DummyRequest({"Authorization": f"Bearer {token}"})
    context = await authenticator.authenticate_request(req)  # type: ignore
    assert context.principal_id == "user123"


@pytest.mark.asyncio
async def test_jwt_validation_invalid_signature() -> None:
    """Test validation of a token signed with the wrong key."""

    authenticator = GatewayAuthenticator(
        database_url="",
        jwt_secret="correct-secret",
    )

    payload = {
        "sub": "user123",
        "exp": (datetime.now(timezone.utc) + timedelta(minutes=5)).timestamp(),
    }
    token = jwt.encode(payload, "wrong-secret", algorithm="HS256")

    req = DummyRequest({"Authorization": f"Bearer {token}"})
    with pytest.raises(AuthenticationError) as exc:
        await authenticator.authenticate_request(req)  # type: ignore
    assert "invalid" in str(exc.value)


@pytest.mark.asyncio
async def test_api_key_validation(clean_postgres, database_url: str) -> None:
    """Test validation of an API key stored in the Postgres database."""

    authenticator = GatewayAuthenticator(
        database_url=database_url,
        jwt_secret="secret",
        api_key_hash_salt="salt",
    )
    await authenticator.startup()

    try:
        conn = clean_postgres
        user_id = await conn.fetchval(
            "INSERT INTO gateway_users (subject, is_active) VALUES ($1, $2) RETURNING id",
            "machine-client",
            True,
        )

        raw_key = "my-secret-key"
        key_hash = _hash_api_key(raw_key, "salt")
        await conn.execute(
            "INSERT INTO gateway_api_keys (user_id, key_name, key_hash, is_active) VALUES ($1, $2, $3, $4)",
            user_id,
            "test-key-name",
            key_hash,
            True,
        )

        req = DummyRequest({"X-API-Key": raw_key})
        context = await authenticator.authenticate_request(req)  # type: ignore

        assert context.principal_id == "machine-client"
        assert context.auth_type == "api_key"
        assert context.credential_id == "test-key-name"

    finally:
        await authenticator.shutdown()
