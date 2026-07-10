"""Map the typed error hierarchy onto HTTP responses."""

from __future__ import annotations

import structlog
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from phronexus.errors import PhronexusError

log = structlog.get_logger("phronexus.api")


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(PhronexusError)
    async def _handle(request: Request, exc: PhronexusError) -> JSONResponse:
        if exc.http_status >= 500:
            log.error("api.error", code=exc.code, detail=str(exc))
        else:
            log.info("api.rejected", code=exc.code, detail=str(exc))
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.code, "detail": str(exc)},
        )
