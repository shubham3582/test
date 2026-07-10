"""Application factory: wire routers, auth, middleware, error handling, OTel."""

from __future__ import annotations

import contextlib
from typing import Optional

import structlog
from fastapi import FastAPI

from phronexus.api.auth import Authenticator
from phronexus.api.errors import install_error_handlers
from phronexus.api.middleware import RequestContextMiddleware
from phronexus.api.routers import contracts, data, health
from phronexus.config import Settings
from phronexus.core import Phronexus

log = structlog.get_logger("phronexus.api")


def create_app(px: Optional[Phronexus] = None, settings: Optional[Settings] = None) -> FastAPI:
    px = px or Phronexus(settings)
    settings = px.settings

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        px.close()

    app = FastAPI(title="Phronexus Core", version="0.1.0", lifespan=lifespan)
    app.state.px = px
    app.state.authenticator = Authenticator(settings.api.auth)

    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)

    app.include_router(health.router)
    app.include_router(data.router)
    app.include_router(contracts.router)

    if settings.observability.otel_enabled:  # pragma: no cover - needs the SDK
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument_app(app)
        except Exception:  # noqa: BLE001
            log.warning("otel.fastapi_instrumentation_unavailable")

    log.info("api.ready", schemes=settings.api.auth.schemes)
    return app
