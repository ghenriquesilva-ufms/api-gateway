"""Async reverse-proxy request forwarding for the API gateway.

This module owns the mechanical work of forwarding an authenticated,
rate-limited request to the correct upstream service and streaming its
response back to the original caller without buffering the body in memory.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Iterable

import httpx
from fastapi import Request
from starlette.responses import StreamingResponse

LOGGER = logging.getLogger(__name__)

# Headers that must not be forwarded by a reverse proxy because they describe
# the transport-level connection between *adjacent* hops, not end-to-end.
# Forwarding them would confuse the upstream (e.g., it might close the
# keep-alive connection it has with the gateway) or expose internal routing.
_HOP_BY_HOP_HEADERS: frozenset[str] = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        # host is set by httpx from the target URL; forwarding the client's
        # value would route the upstream request to the wrong virtual host.
        "host",
    }
)


def _filter_request_headers(
    headers: Iterable[tuple[str, str]],
    client_host: str | None,
) -> list[tuple[str, str]]:
    """Return a clean set of headers suitable for forwarding to an upstream.

    Hop-by-hop headers are stripped so the upstream sees only application-level
    headers. An X-Forwarded-For entry is appended so the upstream can observe
    the original client IP if needed for logging or geo-gating.
    """

    filtered = [
        (name, value)
        for name, value in headers
        if name.lower() not in _HOP_BY_HOP_HEADERS
    ]
    if client_host:
        filtered.append(("x-forwarded-for", client_host))
    return filtered


def _filter_response_headers(
    headers: Iterable[tuple[bytes, bytes]],
) -> list[tuple[str, str]]:
    """Strip hop-by-hop headers from an upstream response before relaying it.

    The upstream's transport metadata is irrelevant to the original client;
    forwarding it would cause protocol-level confusion (e.g., the client
    honouring a keep-alive directive that refers to the gateway→upstream leg).
    """

    return [
        (name.decode(), value.decode())
        for name, value in headers
        if name.decode().lower() not in _HOP_BY_HOP_HEADERS
    ]


# === STUDY-NOTE START ===
# WHAT THIS DOES: Streams the upstream response body back to the caller one
# chunk at a time instead of reading the whole body into a Python bytes object
# first.  Starlette's StreamingResponse accepts an async generator and writes
# each chunk directly to the client socket as it arrives.
# WHY THIS APPROACH: Buffering the full body in memory would cap the gateway's
# throughput at (RAM / body_size) concurrent requests and would add full
# round-trip latency before the client receives its first byte.  Streaming
# keeps memory usage proportional to a single chunk, not the response size,
# and lets the client start consuming data immediately.
# COMMON WRONG IMPLEMENTATION: Calling `response.read()` or
# `await response.aread()` to get the full body, then returning a plain
# JSONResponse or Response — this buffers the entire body and breaks for large
# payloads like file downloads or server-sent events.
# IF YOU'RE STUCK: Use `client.send(built_request, stream=True)` so httpx does
# not buffer the body on its side either, then `async for chunk in
# response.aiter_bytes(): yield chunk` inside the generator.
# === STUDY-NOTE END ===
async def _stream_upstream_response(
    upstream_response: httpx.Response,
) -> AsyncIterator[bytes]:
    """Yield raw byte chunks from an upstream response as they arrive.

    Keeping this as a separate generator makes it easy to unit-test the
    streaming behaviour without spinning up a real HTTP server.
    """

    async for chunk in upstream_response.aiter_bytes():
        yield chunk


async def forward_request(
    request: Request,
    upstream_url: str,
    client: httpx.AsyncClient,
) -> StreamingResponse:
    """Forward an incoming gateway request to an upstream service and stream the response.

    This function is the mechanical core of the reverse proxy.  Policy checks
    (auth, rate limiting, circuit breaker) are enforced by the caller before
    this function is invoked; once here, the only job is faithful, low-latency
    forwarding.

    Args:
        request:      The original FastAPI/Starlette request from the client.
        upstream_url: Fully-qualified URL of the upstream service endpoint,
                      already resolved from the service registry.
        client:       Shared async HTTP client; reusing it preserves the
                      upstream connection pool across requests.

    Returns:
        A ``StreamingResponse`` that relays the upstream status, headers, and
        body chunk-by-chunk to the original caller.
    """

    request_headers = _filter_request_headers(
        request.headers.raw,
        client_host=request.client.host if request.client else None,
    )

    # Build the outbound request, streaming the incoming body so we never
    # materialise it fully in memory on either the read or the write side.
    upstream_request = client.build_request(
        method=request.method,
        url=upstream_url,
        headers=request_headers,
        content=request.stream(),
    )

    LOGGER.debug(
        "proxy_forwarding",
        extra={"upstream_url": upstream_url, "method": request.method},
    )

    upstream_response = await client.send(upstream_request, stream=True)

    response_headers = _filter_response_headers(upstream_response.headers.raw)

    return StreamingResponse(
        content=_stream_upstream_response(upstream_response),
        status_code=upstream_response.status_code,
        headers=dict(response_headers),
        media_type=upstream_response.headers.get("content-type"),
    )
