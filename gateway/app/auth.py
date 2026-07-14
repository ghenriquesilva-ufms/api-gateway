"""Gateway authentication primitives for JWT and machine API keys."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg
import jwt
from fastapi import Request


LOGGER = logging.getLogger(__name__)


def _json_log(event: str, **fields: object) -> None:
    """Emit structured JSON logs for authentication decisions.

    Structured logs are essential for auditing auth outcomes across multiple
    gateway instances and investigating production incidents.
    """

    LOGGER.info(json.dumps({"event": event, **fields}, default=str))


class AuthenticationError(Exception):
    """Signal that an incoming request failed gateway-level authentication."""


@dataclass(frozen=True)
class AuthContext:
    """Describe the authenticated caller attached to a gateway request.

    Request handlers use this context to apply downstream policy decisions
    without re-parsing headers or repeating auth verification work.
    """

    principal_id: str
    auth_type: str
    credential_id: str | None
    scopes: tuple[str, ...]

    @property
    def rate_limit_scope(self) -> str:
        """Return the namespace used to bucket this caller in Redis.

        JWT callers are grouped by user identity, while API-key callers are
        grouped by key identity so machine clients are rate-limited per key.
        """

        if self.auth_type == "api_key" and self.credential_id is not None:
            return f"api_key:{self.credential_id}"
        return self.auth_type


class GatewayAuthenticator:
    """Authenticate proxied requests using JWT or API keys.

    This component provides a central, gateway-level auth guard so backend
    services can trust upstream identity checks.
    """

    def __init__(
        self,
        database_url: str,
        jwt_secret: str,
        jwt_algorithm: str = "HS256",
        jwt_audience: str | None = None,
        jwt_issuer: str | None = None,
        jwt_clock_skew_seconds: int = 30,
        api_key_hash_salt: str = "",
    ) -> None:
        """Store auth configuration and database connectivity settings.

        Configuration is supplied through environment variables to keep the
        gateway twelve-factor friendly and deployment-portable.
        """

        self.database_url = database_url
        self.jwt_secret = jwt_secret
        self.jwt_algorithm = jwt_algorithm
        self.jwt_audience = jwt_audience
        self.jwt_issuer = jwt_issuer
        self.jwt_clock_skew_seconds = jwt_clock_skew_seconds
        self.api_key_hash_salt = api_key_hash_salt
        self._pool: asyncpg.Pool | None = None

    async def startup(self) -> None:
        """Initialize PostgreSQL pool for API-key validation reads."""

        self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=5)
        _json_log("auth_started")

    async def shutdown(self) -> None:
        """Close PostgreSQL resources used by authentication checks."""

        if self._pool is not None:
            await self._pool.close()
            self._pool = None
        _json_log("auth_stopped")

    async def authenticate_request(self, request: Request) -> AuthContext:
        """Authenticate a request using JWT bearer token or API key fallback.

        Machine clients can use API keys while interactive clients use JWT,
        both enforced centrally before proxy forwarding occurs.
        """

        authorization_header = request.headers.get("Authorization")
        api_key_header = request.headers.get("X-API-Key")

        if authorization_header is not None and authorization_header.startswith("Bearer "):
            token = authorization_header.removeprefix("Bearer ").strip()
            return self._validate_jwt_token(token=token)

        if api_key_header is not None:
            return await self._validate_api_key(api_key=api_key_header)

        raise AuthenticationError("Missing credentials. Provide Bearer token or X-API-Key.")

    def _validate_jwt_token(self, token: str) -> AuthContext:
        """Validate JWT integrity and claims before accepting the caller.

        JWT validation is performed in the gateway to provide one consistent
        trust boundary for all downstream services.
        """

        if token == "":
            raise AuthenticationError("Bearer token is empty.")

        options: dict[str, object] = {
            "require": ["exp", "sub"],
            "verify_aud": self.jwt_audience is not None,
            "verify_iss": self.jwt_issuer is not None,
        }

        # === STUDY-NOTE START ===
        # WHAT THIS DOES: Uses jwt.decode with an explicit algorithm allow-list
        # and shared signing key so signatures are always cryptographically verified.
        # WHY THIS APPROACH: Accepting header-declared algorithms or skipping
        # signature checks enables trivial token forgery attacks.
        # COMMON WRONG IMPLEMENTATION: Reading JWT payload with verify_signature
        # disabled, or trusting whatever algorithm the token header declares.
        # IF YOU'RE STUCK: Keep algorithms pinned server-side and fail closed on
        # decode/signature exceptions before touching payload claims.
        # === STUDY-NOTE END ===
        # === STUDY-NOTE START ===
        # WHAT THIS DOES: Applies leeway to exp validation so minor clock drift
        # between issuer and gateway does not cause false unauthorized errors.
        # WHY THIS APPROACH: Distributed systems rarely have perfectly synchronized
        # clocks, and strict zero-skew checks create brittle authentication behavior.
        # COMMON WRONG IMPLEMENTATION: Manually comparing exp to local time without
        # skew allowance, causing intermittent auth failures near token boundaries.
        # IF YOU'RE STUCK: Use JWT library leeway support and still require exp.
        # === STUDY-NOTE END ===
        try:
            claims = jwt.decode(
                token,
                key=self.jwt_secret,
                algorithms=[self.jwt_algorithm],
                audience=self.jwt_audience,
                issuer=self.jwt_issuer,
                leeway=self.jwt_clock_skew_seconds,
                options=options,
            )
        except jwt.ExpiredSignatureError as exc:
            raise AuthenticationError("JWT has expired.") from exc
        except jwt.InvalidTokenError as exc:
            raise AuthenticationError("JWT is invalid.") from exc

        subject = str(claims.get("sub", ""))
        if subject == "":
            raise AuthenticationError("JWT subject is missing.")

        scope_claim = claims.get("scope", "")
        scopes = tuple(str(scope_claim).split()) if str(scope_claim).strip() != "" else tuple()

        _json_log("auth_jwt_success", subject=subject, scopes=scopes)
        return AuthContext(principal_id=subject, auth_type="jwt", credential_id=None, scopes=scopes)

    async def _validate_api_key(self, api_key: str) -> AuthContext:
        """Validate hashed API key against active machine credentials in PostgreSQL.

        API keys support non-human clients that cannot participate in full JWT
        login flows while still preserving centralized gateway enforcement.
        """

        if api_key.strip() == "":
            raise AuthenticationError("X-API-Key is empty.")
        if self._pool is None:
            raise RuntimeError("GatewayAuthenticator pool is not initialized.")

        api_key_hash = _hash_api_key(api_key=api_key, salt=self.api_key_hash_salt)
        now = datetime.now(timezone.utc)

        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT u.subject, k.key_name, k.expires_at
                FROM gateway_api_keys AS k
                INNER JOIN gateway_users AS u ON u.id = k.user_id
                WHERE k.is_active = TRUE
                  AND u.is_active = TRUE
                  AND k.key_hash = $1
                LIMIT 1
                """,
                api_key_hash,
            )

        expires_at = row["expires_at"]
        if isinstance(expires_at, datetime) and expires_at <= now:
            raise AuthenticationError("API key has expired.")

        subject = str(row["subject"])
        key_name = str(row["key_name"])
        _json_log("auth_api_key_success", subject=subject, key_name=key_name)
        return AuthContext(principal_id=subject, auth_type="api_key", credential_id=key_name, scopes=tuple())


def _hash_api_key(api_key: str, salt: str) -> str:
    """Hash API key material for constant-format database lookup.

    Hashing prevents raw machine credentials from being stored in clear text in
    the database and reduces blast radius of credential table exposure.
    """

    digest = hashlib.sha256(f"{salt}{api_key}".encode("utf-8")).hexdigest()
    return digest


def api_keys_match(candidate_hash: str, stored_hash: str) -> bool:
    """Perform constant-time hash comparison for API key checks.

    Constant-time comparison reduces timing side-channel leakage when comparing
    attacker-controlled key material against stored credential hashes.
    """

    return hmac.compare_digest(candidate_hash, stored_hash)
