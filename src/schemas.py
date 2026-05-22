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
    {
        "date_key",
        "store_id",
        "net_sales_total",
        "transactions_total",
        "labor_cost_pct",
    }
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
    "labor_cost_pct",
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
    labor_cost_pct:
        Labor cost as a fraction of net sales for the store-day. Carried
        through to the target schema as ``labor_cost_pct``; ``NaN`` when
        the source cell is empty.
    source_path:
        Absolute or repo-relative path to the source CSV, carried through
        so errors raised downstream can identify the offending file.
    """

    date: date
    store_id: int
    net_sales_total: float
    transactions_total: int
    labor_cost_pct: float
    source_path: str


DEPARTMENT_SALES_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "date_key",
        "store_id",
        "department_id",
        "net_sales",
        "transactions",
        "units_sold",
        "gross_margin_pct",
    }
)
"""Columns the adapter must find in every ``department_sales.csv``.

Extra columns (``gross_sales``, ``cogs``, ``discount_amount``,
``discount_rate``, ``promo_flag``, ``avg_ticket``, ``gross_margin``)
are silently ignored so additions to the sim engine's output do not
break ingestion.
"""

DEPARTMENT_DAILY_METRICS_COLUMNS: tuple[str, ...] = (
    "date",
    "store_id",
    "department_id",
    "net_sales",
    "transactions",
    "units_sold",
    "gross_margin_pct",
)
"""Ordered target schema written to ``department_daily_metrics.parquet``."""

DIM_STORES_FULL_COLUMNS: tuple[str, ...] = (
    "store_id",
    "store_name",
    "address",
    "city",
    "zip",
    "county_fips",
    "trade_area_profile",
    "sqft",
    "open_date",
    "base_daily_revenue",
)
"""Ordered output schema for ``dim_stores.parquet``.

Distinct from :data:`DIM_STORES_REQUIRED_COLUMNS`, which is the
validation contract for the source adapter (only ``store_id`` is
required to be present and coerced). This constant defines the full
column ordering of the canonical artifact written by ``sim_cli``.
"""


class DepartmentSalesRecord(NamedTuple):
    """One row of a sim engine ``department_sales.csv`` after parsing.

    Attributes
    ----------
    date:
        The observation date (``date_key`` parsed from ``YYYY-MM-DD``).
    store_id:
        Integer store identifier (1 through 8 in the current sim engine).
    department_id:
        Integer department identifier (1 through 10 in the current sim
        engine; see ``dim_departments.csv`` for names).
    net_sales:
        Net sales for the store-day-department triple.
    transactions:
        Transaction count involving items from this department on the
        store-day.
    units_sold:
        Total units sold across the department on the store-day.
    gross_margin_pct:
        Gross margin as a fraction (``0.32`` represents 32%); preserved
        as emitted by the sim engine.
    source_path:
        Absolute or repo-relative path to the source CSV, carried through
        so errors raised downstream can identify the offending file.
    """

    date: date
    store_id: int
    department_id: int
    net_sales: float
    transactions: int
    units_sold: int
    gross_margin_pct: float
    source_path: str


ANOMALY_FLAG_COLUMNS: tuple[str, ...] = (
    "date",
    "store_id",
    "rule_id",
    "actual_value",
    "expected_low",
    "expected_high",
    "distance_from_band",
    "severity_score",
    "severity_level",
)
"""Ordered target schema written to ``anomaly_flags.parquet``."""

SEVERITY_LEVELS: tuple[str, ...] = ("info", "warning", "critical")
"""Allowed values for the ``severity_level`` column, ordered low-to-high."""

RULE_IDS: tuple[str, ...] = (
    "revenue_band",
    "labor_pct_band",
    "avg_ticket_band",
    "transactions_band",
    "yoy_comp",
    "department_coverage",
)
"""Allowed values for the ``rule_id`` column, in canonical order.

The first five are statistical-band rules evaluated at store-day grain.
``department_coverage`` is a structural-integrity rule evaluated at
store-day grain against the department-grain metrics frame; it checks
the shape of the data (department row count, duplicate department ids)
rather than whether a value falls inside a band.
"""

KNOWN_PROFILES: frozenset[str] = frozenset(
    {"suburban-family", "urban-dense", "value-market"}
)
"""Trade area profiles defined by the live sim engine seed config."""
