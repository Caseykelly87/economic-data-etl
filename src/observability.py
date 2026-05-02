"""Structlog configuration for the ETL pipeline.

Single entry point: configure_logging(). Called once at startup from
the cli entry points (main.py and sim_cli.py, detect_cli.py if
present). The configuration applies to both structlog loggers
(created via structlog.get_logger()) and the stdlib logging loggers
(created via logging.getLogger()) — they share the same renderer and
level filter through structlog's stdlib bridge.

Output format
-------------
- Auto-detect: console (colored, human-friendly) when stdout is a tty,
  json (single-line, machine-parseable) otherwise.
- Override: set LOG_FORMAT=json or LOG_FORMAT=console.

Log level
---------
- LOG_LEVEL env var (case-insensitive). Defaults to info.
- Valid values: debug, info, warning, error, critical.

Windows note
------------
On Windows, stdout defaults to cp1252 encoding which can't render
the non-ASCII characters used in some pre-existing log strings (emoji
like the warning, skip, and check marks in extract.py). The
configurator reconfigures stdout to utf-8 with errors="replace" to
avoid UnicodeEncodeError when output is piped or redirected.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog


def _resolve_level() -> int:
    raw = os.environ.get("LOG_LEVEL", "info").lower()
    mapping = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    return mapping.get(raw, logging.INFO)


def _resolve_format() -> str:
    raw = os.environ.get("LOG_FORMAT", "").lower()
    if raw in ("json", "console"):
        return raw
    return "console" if sys.stdout.isatty() else "json"


def _add_logger_name_safe(logger, method_name, event_dict):
    """Tolerant of loggers without a .name attribute (e.g. PrintLogger)."""
    name = getattr(logger, "name", None)
    if name is not None:
        event_dict["logger"] = name
    return event_dict


def configure_logging() -> None:
    """Configure structlog and the stdlib logging bridge.

    Called once at startup. Idempotent — calling twice is safe.
    """
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    level = _resolve_level()
    output_format = _resolve_format()

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        _add_logger_name_safe,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if output_format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stdout.isatty())

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=renderer,
            foreign_pre_chain=shared_processors,
        )
    )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
