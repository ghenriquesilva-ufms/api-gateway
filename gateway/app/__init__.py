"""API gateway package containing the gateway entrypoint and its domain modules."""

from .api import create_app

__all__ = ["create_app"]
