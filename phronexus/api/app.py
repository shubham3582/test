"""Application factory: wire routers, auth, middleware, error handling, OTel."""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Optional

import structlog
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from phronexus.api.auth import Authenticator
from phronexus.api.errors import install_error_handlers
from phronexus.api.middleware import RequestContextMiddleware
from phronexus.api.providers import build_provider
from phronexus.api.routers import auth, contracts, data, events, health
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
    app.state.auth_provider = build_provider(settings.api.auth)
    # Synchronous state-machine face shares the engine; outputs fan out via Kafka.
    from phronexus.statemachine.io import build_output_publisher

    app.state.state_machine = px.state_machine(output=build_output_publisher(settings))

    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)

    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(data.router)
    app.include_router(events.router)
    app.include_router(contracts.router)

    # Serve the management UI (self-contained SPA) at /ui.
    ui_dir = Path(__file__).parent / "ui"
    if ui_dir.is_dir():
        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=True), name="ui")

        @app.get("/", include_in_schema=False)
        def _root():
            return RedirectResponse(url="/ui/")

    if settings.observability.otel_enabled:  # pragma: no cover - needs the SDK
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument_app(app)
        except Exception:  # noqa: BLE001
            log.warning("otel.fastapi_instrumentation_unavailable")

    log.info("api.ready", schemes=settings.api.auth.schemes)
    return app
