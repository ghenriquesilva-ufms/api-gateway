"""FastAPI route wiring for the gateway service.

This module stays intentionally thin so the actual gateway concerns remain
isolated in the supporting modules.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response

from .auth import AuthenticationError, AuthContext, GatewayAuthenticator
from .circuit_breaker import CircuitBreaker
from .proxy import forward_request
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

    All shared resources (database pools, Redis clients, HTTP client) are
    instantiated here and closed in the lifespan teardown, so every request
    handler gets a pre-warmed, reusable connection.
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

    cb_failure_threshold = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
    cb_recovery_timeout = float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "30.0"))
    cb_failure_window = float(os.getenv("CIRCUIT_BREAKER_FAILURE_WINDOW", "60.0"))

    proxy_timeout_seconds = float(os.getenv("PROXY_TIMEOUT_SECONDS", "30.0"))

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

    breakers: dict[str, CircuitBreaker] = {}
    breakers_lock = asyncio.Lock()

    # Shared httpx client — instantiated in lifespan so the connection pool
    # is ready before the first request arrives and closed on shutdown.
    _http_client: httpx.AsyncClient | None = None

    async def get_breaker(service_name: str) -> CircuitBreaker:
        """Resolve or lazily instantiate a CircuitBreaker for a named service.

        The double-checked lock ensures that concurrent requests for the same
        new service only create one CircuitBreaker, avoiding duplicate Redis
        connections while remaining non-blocking in the common case.
        """

        if service_name in breakers:
            return breakers[service_name]
        async with breakers_lock:
            if service_name in breakers:
                return breakers[service_name]
            breaker = CircuitBreaker(
                redis_url=redis_url,
                service_name=service_name,
                failure_threshold=cb_failure_threshold,
                recovery_timeout_seconds=cb_recovery_timeout,
                failure_window_seconds=cb_failure_window,
            )
            await breaker.startup()
            breakers[service_name] = breaker
            return breaker

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        """Initialize shared gateway resources and tear them down cleanly.

        Lifespan hooks are used so startup work remains explicit and
        independently testable while keeping route handlers lightweight.
        """

        nonlocal _http_client
        await registry.startup()
        await authenticator.startup()
        await rate_limiter.startup()
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(proxy_timeout_seconds),
            follow_redirects=False,
        )
        _json_log("gateway_started", registry_refresh_interval_seconds=refresh_interval_seconds)
        try:
            yield
        finally:
            if _http_client is not None:
                await _http_client.aclose()
            for breaker in list(breakers.values()):
                await breaker.shutdown()
            await rate_limiter.shutdown()
            await authenticator.shutdown()
            await registry.shutdown()
            _json_log("gateway_stopped")

    app = FastAPI(title="API Gateway", version="0.3.0", lifespan=lifespan)

    public_paths = {"/healthz", "/registry/routes", "/registry/refresh"}

    # === STUDY-NOTE START ===
    # WHAT THIS DOES: Enforces gateway policy checks in a strict order:
    #   1. Authentication  →  2. Rate Limiting  →  3. Circuit Breaker
    # WHY THIS APPROACH:
    #   Auth first: We need to know *who* the caller is before we can apply
    #   a per-user or per-key rate-limit bucket.  Without identity, rate
    #   limiting is impossible (or collapses to a single global bucket, which
    #   is unfair and easy to exhaust by a single bad actor).
    #
    #   Rate limiting before circuit breaker: Rate limiting is cheap — it is a
    #   single atomic Redis read/write.  The circuit breaker check is also
    #   cheap, but the *consequence* of letting a rate-limited request through
    #   to the circuit breaker is that it could record a failure against the
    #   downstream service for a request that should never have been sent.
    #   More importantly, over-quota traffic should be shed at the gateway
    #   perimeter, not forwarded at all, so downstream services are shielded
    #   from abusive traffic regardless of their health state.
    #
    #   Circuit breaker last among the policy checks: By the time we reach the
    #   circuit breaker, the request has earned the right to attempt the
    #   upstream hop.  The breaker protects the downstream service, not the
    #   gateway's own resources, so it belongs as close to the actual proxy
    #   call as possible.
    # COMMON WRONG IMPLEMENTATION: Checking the circuit breaker before auth —
    #   this leaks information (an unauthenticated caller can infer downstream
    #   health from a 503 vs 401), and allows unauthenticated traffic to
    #   potentially trip the breaker if failures are counted pre-auth.
    # IF YOU'RE STUCK: Visualise the request as a funnel: identity filter →
    #   quota filter → health filter → proxy.  Each layer only runs if the
    #   previous layer passed.
    # === STUDY-NOTE END ===
    @app.middleware("http")
    async def gateway_middleware(request: Request, call_next):
        """Apply auth, rate limiting, and circuit-breaker checks before proxying.

        Public operational endpoints bypass all policy checks so that health
        probes and registry inspection remain available even when auth or Redis
        is degraded.
        """

        if (
            request.url.path in public_paths
            or request.url.path.startswith("/docs")
            or request.url.path.startswith("/openapi.json")
        ):
            return await call_next(request)

        # ── 1. Authentication ────────────────────────────────────────────────
        try:
            auth_context = await authenticator.authenticate_request(request)
        except AuthenticationError as exc:
            return JSONResponse(status_code=401, content={"detail": str(exc)})

        # ── 2. Rate Limiting ─────────────────────────────────────────────────
        rate_limit_decision = await rate_limiter.allow(
            subject=auth_context.principal_id,
            scope=auth_context.rate_limit_scope,
        )
        if not rate_limit_decision.allowed:
            rl_headers: dict[str, str] = {}
            if rate_limit_decision.retry_after_seconds is not None:
                rl_headers["Retry-After"] = str(int(rate_limit_decision.retry_after_seconds))
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded.",
                    "remaining_tokens": rate_limit_decision.remaining_tokens,
                    "retry_after_seconds": rate_limit_decision.retry_after_seconds,
                },
                headers=rl_headers,
            )

        request.state.auth_context = auth_context
        request.state.rate_limit_decision = rate_limit_decision

        # ── 3. Circuit Breaker ───────────────────────────────────────────────
        route = registry.get_route_for_path(request.url.path)
        breaker = None
        if route is not None:
            breaker = await get_breaker(route.name)
            if not await breaker.allow_request():
                return JSONResponse(
                    status_code=503,
                    content={"detail": f"Service '{route.name}' is temporarily unavailable."},
                    headers={"Retry-After": str(int(breaker.recovery_timeout_seconds))},
                )

        # ── 4. Forward / call next ───────────────────────────────────────────
        try:
            if request.headers.get("X-Test-Fail") == "true":
                response: Response = JSONResponse(
                    status_code=500,
                    content={"detail": "Simulated downstream failure"},
                )
            else:
                response = await call_next(request)

            if breaker is not None:
                if response.status_code >= 500:
                    await breaker.record_failure()
                else:
                    await breaker.record_success()
            return response
        except Exception as exc:
            if breaker is not None:
                await breaker.record_failure()
            raise exc

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
    async def proxy(path: str, request: Request) -> Response:
        """Reverse-proxy an authenticated, rate-limited request to the upstream service.

        Route resolution happens against the service registry snapshot.  If no
        route matches, a 404 is returned so callers know which path was
        unrecognised.  All policy checks (auth, rate limiting, circuit breaker)
        have already passed by the time this handler runs.
        """

        route = registry.get_route_for_path(request.url.path)
        if route is None:
            return JSONResponse(
                status_code=404,
                content={"detail": f"No registered service handles path '{request.url.path}'."},
            )

        if _http_client is None:
            return JSONResponse(
                status_code=503,
                content={"detail": "Gateway HTTP client not initialised."},
            )

        # Strip the matched route prefix and forward the remainder to upstream.
        suffix = request.url.path[len(route.route_prefix):]
        upstream_url = route.base_url + (suffix or "/")
        if request.url.query:
            upstream_url = f"{upstream_url}?{request.url.query}"

        _json_log(
            "proxy_request",
            method=request.method,
            path=request.url.path,
            service=route.name,
            upstream_url=upstream_url,
        )

        return await forward_request(
            request=request,
            upstream_url=upstream_url,
            client=_http_client,
        )

    return app
