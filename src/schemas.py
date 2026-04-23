"""Column contracts and typed records for the sim engine ingestion flow.

This module defines the boundary between the source adapter
(``sim_ingest``) and the transform (``sim_transform``). Constants here are
the source-of-truth for every schema assertion: the adapter validates
inputs against ``*_REQUIRED_COLUMNS``, the transform emits exactly the
columns in ``STORE_DAILY_METRICS_COLUMNS``, and both sides agree on the
shape of :class:`StoreSummaryRecord`.
"""

from __future__ import annotations

from datetime import date
from typing import NamedTuple

STORE_SUMMARY_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"date_key", "store_id", "net_sales_total", "transactions_total"}
)
"""Columns the adapter must find in every ``store_summary.csv``.

Extra columns (``gross_sales_total``, ``labor_cost``, ...) are silently
ignored so additions to the sim engine's output do not break ingestion.
"""

DIM_STORES_REQUIRED_COLUMNS: frozenset[str] = frozenset({"store_id"})
"""Columns the adapter must find in ``dim_stores.csv``.

Only ``store_id`` is validated; downstream transform uses it purely for
referential validation against records yielded from store summaries.
"""

STORE_DAILY_METRICS_COLUMNS: tuple[str, ...] = (
    "date",
    "store_id",
    "total_sales",
    "transaction_count",
    "avg_basket_size",
)
"""Ordered target schema written to ``store_daily_metrics.parquet``."""


class StoreSummaryRecord(NamedTuple):
    """One row of a sim engine ``store_summary.csv`` after parsing.

    Attributes
    ----------
    date:
        The observation date (``date_key`` parsed from ``YYYY-MM-DD``).
    store_id:
        Integer store identifier (1 through 8 in the current sim engine).
    net_sales_total:
        Net sales after returns and discounts. Mapped to ``total_sales``
        in the target schema.
    transactions_total:
        Count of transactions for the store-day; denominator for
        ``avg_basket_size``.
    source_path:
        Absolute or repo-relative path to the source CSV, carried through
        so errors raised downstream can identify the offending file.
    """

    date: date
    store_id: int
    net_sales_total: float
    transactions_total: int
    source_path: str
