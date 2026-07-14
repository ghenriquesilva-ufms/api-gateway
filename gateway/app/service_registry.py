"""Database-backed service registry primitives for gateway routing.

This module owns registry state hydration from PostgreSQL and keeps a
read-optimized in-memory snapshot that can refresh safely at runtime.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType

import asyncpg


LOGGER = logging.getLogger(__name__)


def _json_log(event: str, **fields: object) -> None:
    """Emit a structured JSON log message through the standard logger.

    Structured JSON logs make service registry behavior easier to debug in
    containerized deployments and machine-processable in log pipelines.
    """

    payload = {"event": event, **fields}
    LOGGER.info(json.dumps(payload, default=str))


def _normalize_path_prefix(prefix: str) -> str:
    """Normalize route prefixes so lookups use a single canonical format.

    A canonical path prefix avoids subtle routing mismatches caused by extra
    slashes or missing leading separators.
    """

    cleaned = "/" + prefix.strip().strip("/")
    return cleaned if cleaned != "/" else "/"


@dataclass(frozen=True)
class ServiceRoute:
    """Describe how a route prefix maps to a downstream service.

    Route records are loaded from PostgreSQL so new routes can be registered
    without redeploying the gateway binary.
    """

    name: str
    route_prefix: str
    base_url: str
    health_check_path: str
    enabled: bool


class ServiceRegistry:
    """Load and query service routing configuration from durable storage.

    The registry periodically refreshes from PostgreSQL while exposing an
    atomic snapshot to request handlers so readers never observe partial state.
    """

    def __init__(self, database_url: str, refresh_interval_seconds: float = 15.0) -> None:
        """Initialize database connection details and refresh behavior.

        The refresh interval controls how quickly config changes in PostgreSQL
        become visible to live gateway workers without a process restart.
        """

        self.database_url = database_url
        self.refresh_interval_seconds = refresh_interval_seconds
        self._pool: asyncpg.Pool | None = None
        self._refresh_task: asyncio.Task[None] | None = None
        self._snapshot_lock = asyncio.Lock()
        self._routes_by_prefix: MappingProxyType[str, ServiceRoute] = MappingProxyType({})
        self._last_refresh_at: datetime | None = None

    async def startup(self, max_attempts: int = 10, retry_delay_seconds: float = 1.5) -> None:
        """Connect to PostgreSQL, perform initial load, and start auto-refresh.

        Startup retries are required because compose may start the gateway before
        PostgreSQL is fully ready to accept client connections.
        """

        for attempt in range(1, max_attempts + 1):
            try:
                self._pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=5)
                await self._refresh_once(reason="startup")
                self._refresh_task = asyncio.create_task(self._refresh_loop())
                _json_log("service_registry_started", attempt=attempt, refresh_interval_seconds=self.refresh_interval_seconds)
                return
            except (asyncpg.PostgresError, OSError) as exc:
                _json_log("service_registry_startup_retry", attempt=attempt, max_attempts=max_attempts, error=str(exc))
                if attempt == max_attempts:
                    raise
                await asyncio.sleep(retry_delay_seconds)

    async def shutdown(self) -> None:
        """Stop auto-refresh and close active database resources gracefully.

        Graceful shutdown prevents task leaks and avoids noisy cancellation logs
        during local compose shutdown and container restarts.
        """

        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None

        if self._pool is not None:
            await self._pool.close()
            self._pool = None

        _json_log("service_registry_stopped")

    async def list_routes(self) -> list[ServiceRoute]:
        """Return the currently active route snapshot used by request handlers.

        Returning from the in-memory snapshot keeps per-request registry reads
        non-blocking and independent from database latency.
        """

        return list(self._routes_by_prefix.values())

    def get_route_for_path(self, path: str) -> ServiceRoute | None:
        """Resolve an incoming path to the best matching configured route.

        Prefix matching enables a single service registration to handle an
        entire subtree such as ``/alpha/*`` or ``/payments/*``.
        """

        normalized_path = _normalize_path_prefix(path)
        route_prefixes = sorted(self._routes_by_prefix.keys(), key=len, reverse=True)
        for prefix in route_prefixes:
            if normalized_path == prefix or normalized_path.startswith(prefix + "/"):
                return self._routes_by_prefix[prefix]
        return None

    async def force_refresh(self) -> None:
        """Trigger an immediate refresh cycle outside the periodic scheduler.

        Manual refresh is useful for administration endpoints and test flows
        that need deterministic visibility of just-written route records.
        """

        await self._refresh_once(reason="manual")

    def last_refresh_iso(self) -> str | None:
        """Return the last successful refresh timestamp in ISO-8601 format.

        This value helps operators confirm that periodic refresh is still active
        and that the gateway is not serving stale registry data.
        """

        if self._last_refresh_at is None:
            return None
        return self._last_refresh_at.isoformat()

    async def _refresh_loop(self) -> None:
        """Continuously refresh registry state from PostgreSQL at fixed intervals."""

        while True:
            await asyncio.sleep(self.refresh_interval_seconds)
            try:
                await self._refresh_once(reason="periodic")
            except (asyncpg.PostgresError, OSError) as exc:
                _json_log("service_registry_refresh_failed", error=str(exc))

    async def _refresh_once(self, reason: str) -> None:
        """Fetch route records and atomically publish them as the active snapshot."""

        if self._pool is None:
            raise RuntimeError("ServiceRegistry pool is not initialized.")

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT name, route_prefix, base_url, health_check_path, is_enabled
                FROM gateway_services
                WHERE is_enabled = TRUE
                ORDER BY route_prefix ASC
                """
            )

        new_snapshot: dict[str, ServiceRoute] = {}
        for row in rows:
            route = ServiceRoute(
                name=str(row["name"]),
                route_prefix=_normalize_path_prefix(str(row["route_prefix"])),
                base_url=str(row["base_url"]).rstrip("/"),
                health_check_path=_normalize_path_prefix(str(row["health_check_path"])),
                enabled=bool(row["is_enabled"]),
            )
            new_snapshot[route.route_prefix] = route

        # === STUDY-NOTE START ===
        # WHAT THIS DOES: Builds a complete fresh snapshot, then swaps the live
        # mapping in one lock-protected assignment.
        # WHY THIS APPROACH: Readers either observe the old full snapshot or the
        # new full snapshot, never an in-between partially refreshed map.
        # COMMON WRONG IMPLEMENTATION: Mutating a shared dict in place while
        # requests read it, which can expose half-updated state and flapping routes.
        # IF YOU'RE STUCK: Use copy-on-write + single-pointer swap; avoid in-place
        # mutation of structures that concurrent readers iterate over.
        # === STUDY-NOTE END ===
        async with self._snapshot_lock:
            self._routes_by_prefix = MappingProxyType(new_snapshot)
            self._last_refresh_at = datetime.now(timezone.utc)

        _json_log("service_registry_refreshed", reason=reason, route_count=len(new_snapshot), refreshed_at=self.last_refresh_iso())
