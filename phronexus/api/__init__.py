"""REST API (FastAPI) exposing the same core as the Python SDK."""

from phronexus.api.app import create_app

__all__ = ["create_app"]
