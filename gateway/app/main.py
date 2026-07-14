"""Application entrypoint for the API gateway service."""

from .api import create_app

app = create_app()
