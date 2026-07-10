"""Central structured-logging configuration.

All modules use ``structlog.get_logger(__name__)``; this function is the single
place that decides format (JSON vs console), level and shared processors, so log
behaviour can be changed for the whole platform in one call.
"""

from __future__ import annotations

import logging

import structlog

from phronexus.config import ObservabilitySettings


def configure_logging(cfg: ObservabilitySettings) -> None:
    level = getattr(logging, cfg.log_level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", level=level)

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    processors.append(
        structlog.processors.JSONRenderer()
        if cfg.log_json
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=cfg.service_name)
