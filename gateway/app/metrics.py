"""Prometheus instrumentation helpers for the gateway."""

from __future__ import annotations

from prometheus_client import CollectorRegistry


def build_registry() -> CollectorRegistry:
    """Create a dedicated Prometheus registry for the gateway metrics."""

    raise NotImplementedError("Prometheus metric wiring will be implemented later.")
