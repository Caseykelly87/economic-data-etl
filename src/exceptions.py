"""Typed exceptions raised by the sim engine ingestion flow.

All exceptions inherit from :class:`SimIngestError` so callers can catch the
entire failure family with a single handler while still distinguishing the
schema-vs-reconciliation distinction when useful.
"""

from __future__ import annotations

from typing import Any


class SimIngestError(Exception):
    """Base class for every sim engine ingestion failure.

    Accepts a human-readable ``message`` and arbitrary keyword context
    (``path``, ``column``, ``store_id``, ``row_count``, ...) that is stored
    on the instance for programmatic inspection and echoed into ``str(exc)``
    so log output identifies the offending file or row without additional
    plumbing.
    """

    def __init__(self, message: str, **context: Any) -> None:
        self.message = message
        self.context = context
        super().__init__(self._format(message, context))

    @staticmethod
    def _format(message: str, context: dict[str, Any]) -> str:
        if not context:
            return message
        rendered = ", ".join(f"{k}={v!r}" for k, v in context.items())
        return f"{message} ({rendered})"


class SchemaValidationError(SimIngestError):
    """Raised when a source CSV fails structural validation.

    Covers: required columns missing from a ``store_summary.csv`` or
    ``dim_stores.csv`` header, unparseable field types, and referential
    violations such as a ``store_id`` in ``store_summary`` that is absent
    from ``dim_stores``.
    """


class ReconciliationError(SimIngestError):
    """Raised when row counts or file presence fail the reconciliation rule.

    Covers: a walked date directory missing its ``store_summary.csv``,
    no date directories found under ``daily/``, and output row counts that
    do not equal the sum of input rows.
    """


class DetectionError(Exception):
    """Base class for every exception-detection failure.

    Mirrors :class:`SimIngestError`'s message + ``**context`` pattern so
    detection callers get the same operator-friendly error formatting.
    """

    def __init__(self, message: str, **context: Any) -> None:
        self.message = message
        self.context = context
        super().__init__(self._format(message, context))

    @staticmethod
    def _format(message: str, context: dict[str, Any]) -> str:
        if not context:
            return message
        rendered = ", ".join(f"{k}={v!r}" for k, v in context.items())
        return f"{message} ({rendered})"


class DetectionConfigError(DetectionError):
    """Raised when ``config/detection_rules.yaml`` fails validation.

    Covers: a required section is missing, a profile referenced in
    ``bands_by_profile`` is not in :data:`schemas.KNOWN_PROFILES`, or
    a rule's required keys are absent.
    """


class DetectionInputError(DetectionError):
    """Raised when input parquet or dim_stores fails structural checks.

    Covers: ``store_daily_metrics.parquet`` missing, unreadable, or
    missing a column from :data:`schemas.STORE_DAILY_METRICS_COLUMNS`;
    a ``store_id`` referenced in the metrics frame absent from
    ``dim_stores``.
    """
