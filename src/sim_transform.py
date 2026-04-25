"""Pure-pandas normalization from sim engine records to the target schema.

This module has zero filesystem, CSV, or transport dependencies. Its
only imports are pandas plus the internal schema/exception contracts.
That boundary is what allows ``sim_ingest`` to be swapped for a
different adapter (e.g. Google Sheets) without touching normalization.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from src.exceptions import SchemaValidationError
from src.schemas import STORE_DAILY_METRICS_COLUMNS, StoreSummaryRecord


def build_store_daily_metrics(
    summaries: Iterable[StoreSummaryRecord],
    dim_stores: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize sim engine records into the ``store_daily_metrics`` frame.

    Every ``store_id`` present in ``summaries`` must exist in
    ``dim_stores["store_id"]``; orphans raise :class:`SchemaValidationError`
    listing the offending ids so operators can pinpoint the divergence.

    ``avg_basket_size`` is computed as ``total_sales / transactions_total``
    with ``NaN`` (not an exception) when ``transactions_total == 0`` —
    zero-transaction store-days are a legitimate business state (holiday
    closure, opening day before register open) and must not abort ingest.

    Rows are sorted by ``(date, store_id)`` so repeat runs on identical
    input produce byte-identical parquet output downstream.
    """
    records = list(summaries)
    known_store_ids = set(dim_stores["store_id"].astype(int))

    if records:
        orphan_ids = sorted(
            {r.store_id for r in records} - known_store_ids
        )
        if orphan_ids:
            raise SchemaValidationError(
                "store_summary references store_ids not in dim_stores",
                orphan_store_ids=orphan_ids,
            )

    df = pd.DataFrame(
        {
            "date": [r.date for r in records],
            "store_id": np.array([r.store_id for r in records], dtype=np.int64),
            "total_sales": np.array(
                [r.net_sales_total for r in records], dtype=np.float64
            ),
            "transaction_count": np.array(
                [r.transactions_total for r in records], dtype=np.int64
            ),
            "labor_cost_pct": np.array(
                [r.labor_cost_pct for r in records], dtype=np.float64
            ),
        }
    )

    with np.errstate(divide="ignore", invalid="ignore"):
        df["avg_basket_size"] = np.where(
            df["transaction_count"] > 0,
            df["total_sales"] / df["transaction_count"],
            np.nan,
        )

    # Closed-day rows (zero net sales) carry no meaningful labor pct;
    # blank them so downstream rules skip rather than band-check 0.
    df.loc[df["total_sales"] == 0, "labor_cost_pct"] = np.nan

    df = df[list(STORE_DAILY_METRICS_COLUMNS)]
    df = df.sort_values(["date", "store_id"]).reset_index(drop=True)
    return df
