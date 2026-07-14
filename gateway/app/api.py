"""FastAPI route wiring for the gateway service.

This module stays intentionally thin so the actual gateway concerns remain
isolated in the supporting modules.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .auth import AuthenticationError, AuthContext, GatewayAuthenticator
from .rate_limiter import TokenBucketRateLimiter
from .service_registry import ServiceRegistry


LOGGER = logging.getLogger(__name__)


def _json_log(event: str, **fields: object) -> None:
    """Emit a structured JSON log message using the standard logger.

    JSON logs keep gateway lifecycle events easy to correlate across
    distributed environments and container logs.
    """

    LOGGER.info(json.dumps({"event": event, **fields}, default=str))


def create_app() -> FastAPI:
    """Create and configure the FastAPI application for the gateway.

    The app is kept lightweight in this phase so the container stack can run
    end to end while the business logic is added in later iterations.
    """

    database_url = os.getenv("DATABASE_URL")
    if database_url is None:
        raise RuntimeError("DATABASE_URL must be set for the service registry.")

    jwt_secret = os.getenv("JWT_SECRET", "dev-jwt-secret")
    jwt_issuer = os.getenv("JWT_ISSUER")
    jwt_audience = os.getenv("JWT_AUDIENCE")
    jwt_clock_skew_seconds = int(os.getenv("JWT_CLOCK_SKEW_SECONDS", "30"))
    api_key_hash_salt = os.getenv("API_KEY_HASH_SALT", "")
    redis_url = os.getenv("REDIS_URL")
    if redis_url is None:
        raise RuntimeError("REDIS_URL must be set for the rate limiter.")

    rate_limit_capacity = float(os.getenv("RATE_LIMIT_CAPACITY", "10"))
    rate_limit_refill_rate_per_second = float(os.getenv("RATE_LIMIT_REFILL_RATE_PER_SECOND", "5"))
    refresh_interval_seconds = float(os.getenv("SERVICE_REGISTRY_REFRESH_SECONDS", "15"))
    registry = ServiceRegistry(
        database_url=database_url,
        refresh_interval_seconds=refresh_interval_seconds,
    )
    authenticator = GatewayAuthenticator(
        database_url=database_url,
        jwt_secret=jwt_secret,
        jwt_issuer=jwt_issuer,
        jwt_audience=jwt_audience,
        jwt_clock_skew_seconds=jwt_clock_skew_seconds,
        api_key_hash_salt=api_key_hash_salt,
    )
    rate_limiter = TokenBucketRateLimiter(
        redis_url=redis_url,
        capacity=rate_limit_capacity,
        refill_rate_per_second=rate_limit_refill_rate_per_second,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """Initialize shared gateway resources and tear them down cleanly.

        Lifespan hooks are used so startup work remains explicit and
        independently testable while keeping route handlers lightweight.
        """

        await registry.startup()
        await authenticator.startup()
        await rate_limiter.startup()
        _json_log("gateway_started", registry_refresh_interval_seconds=refresh_interval_seconds)
        try:
            yield
        finally:
            await rate_limiter.shutdown()
            await authenticator.shutdown()
            await registry.shutdown()
            _json_log("gateway_stopped")

    app = FastAPI(title="API Gateway", version="0.2.0", lifespan=lifespan)

    public_paths = {"/healthz", "/registry/routes", "/registry/refresh"}

    @app.middleware("http")
    async def authenticate_requests(request: Request, call_next):
        """Authenticate every proxied request before route dispatch.

        Public operational endpoints remain available for health checks and
        registry inspection, but everything that can reach the proxy path must
        pass through gateway auth first.
        """

        if request.url.path in public_paths or request.url.path.startswith("/docs") or request.url.path.startswith("/openapi.json"):
            return await call_next(request)

        try:
            auth_context = await authenticator.authenticate_request(request)
        except AuthenticationError as exc:
            return JSONResponse(status_code=401, content={"detail": str(exc)})

        rate_limit_decision = await rate_limiter.allow(
            subject=auth_context.principal_id,
            scope=auth_context.rate_limit_scope,
        )
        if not rate_limit_decision.allowed:
            headers = {}
            if rate_limit_decision.retry_after_seconds is not None:
                headers["Retry-After"] = str(int(rate_limit_decision.retry_after_seconds))
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded.",
                    "remaining_tokens": rate_limit_decision.remaining_tokens,
                    "retry_after_seconds": rate_limit_decision.retry_after_seconds,
                },
                headers=headers,
            )

        request.state.auth_context = auth_context
        request.state.rate_limit_decision = rate_limit_decision
        return await call_next(request)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Return a basic liveness response for container and smoke checks."""

        return {"status": "ok", "service": "gateway"}

    @app.get("/registry/routes")
    async def list_registry_routes() -> dict[str, Any]:
        """Return the active in-memory registry snapshot.

        This endpoint provides observability to confirm runtime refresh has
        loaded route updates from PostgreSQL without gateway redeploys.
        """

        routes = await registry.list_routes()
        return {
            "routes": [
                {
                    "name": route.name,
                    "route_prefix": route.route_prefix,
                    "base_url": route.base_url,
                    "health_check_path": route.health_check_path,
                }
                for route in routes
            ],
            "last_refresh_at": registry.last_refresh_iso(),
        }

    @app.post("/registry/refresh")
    async def refresh_registry() -> dict[str, str | None]:
        """Trigger a manual registry refresh from PostgreSQL.

        A manual trigger is helpful during operational verification and tests
        that validate dynamic route changes immediately.
        """

        await registry.force_refresh()
        return {"status": "ok", "last_refresh_at": registry.last_refresh_iso()}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def placeholder_proxy(path: str, request: Request) -> dict[str, Any]:
        """Return a placeholder response for now instead of performing proxying.

        The real reverse-proxy logic will be added after the auth, registry,
        rate limiting, circuit breaker, and metrics modules are in place.
        """

        route = registry.get_route_for_path(path)
        auth_context = getattr(request.state, "auth_context", None)

        return {
            "service": "gateway",
            "message": "gateway skeleton response",
            "method": request.method,
            "path": path,
            "matched_service": route.name if route is not None else None,
            "matched_upstream_url": route.base_url if route is not None else None,
            "auth_principal": auth_context.principal_id if isinstance(auth_context, AuthContext) else None,
            "auth_type": auth_context.auth_type if isinstance(auth_context, AuthContext) else None,
            "remaining_rate_limit_tokens": (
                getattr(request.state, "rate_limit_decision", None).remaining_tokens
                if getattr(request.state, "rate_limit_decision", None) is not None
                else None
            ),
        }

    return app
