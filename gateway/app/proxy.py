"""Request forwarding helpers for the gateway reverse proxy."""

from __future__ import annotations

from fastapi import Request
from starlette.responses import Response


async def forward_request(request: Request, upstream_url: str) -> Response:
    """Forward an incoming request to an upstream service.

    The real implementation will preserve method, headers, body, and response
    metadata while applying gateway policies first.
    """

    raise NotImplementedError("Proxy forwarding will be implemented in a later phase.")
