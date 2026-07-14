"""Minimal FastAPI backend used to validate gateway routing in Compose."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import FastAPI, Request


def create_app() -> FastAPI:
    """Create a small backend app that echoes its own name after a delay."""

    service_name = os.getenv("SERVICE_NAME", "dummy-service")
    fake_latency_ms = int(os.getenv("FAKE_LATENCY_MS", "25"))

    app = FastAPI(title=f"{service_name} backend", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Return a liveness response for the dummy backend container."""

        return {"status": "ok", "service": service_name}

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def echo(path: str, request: Request) -> dict[str, Any]:
        """Echo the backend identity and wait briefly to simulate real latency."""

        await asyncio.sleep(fake_latency_ms / 1000)
        return {
            "service": service_name,
            "message": "dummy backend response",
            "method": request.method,
            "path": path,
            "fake_latency_ms": fake_latency_ms,
        }

    return app


app = create_app()
