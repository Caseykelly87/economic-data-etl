"""Source adapter for the grocery simulation engine's output tree.

This module owns every piece of source-format knowledge: where files live,
how the directory tree is laid out, how CSV rows are parsed, and how type
coercion is applied. Its public surface is intentionally narrow — two
functions returning typed records or a DataFrame — so that a future
alternate-transport adapter (e.g. a Google Sheets reader) can be swapped
in without any change to downstream ``sim_transform`` or ``sim_cli``.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Iterator

import pandas as pd

from src.exceptions import ReconciliationError, SchemaValidationError
from src.schemas import (
    DIM_STORES_REQUIRED_COLUMNS,
    STORE_SUMMARY_REQUIRED_COLUMNS,
    StoreSummaryRecord,
)


def load_store_summaries(root: Path) -> Iterator[StoreSummaryRecord]:
    """Walk ``{root}/daily/{MM}/{DD}/{YYYY}/`` and yield one record per CSV row.

    The walker sorts matched paths so yield order is deterministic across
    runs and platforms. Every ``store_summary.csv`` is validated against
    :data:`STORE_SUMMARY_REQUIRED_COLUMNS`; extra columns are silently
    ignored so additions to the sim engine's output do not break ingestion.

    Parameters
    ----------
    root:
        Path to the sim engine's ``output/`` directory. The function
        expects ``output/daily/`` beneath it.

    Yields
    ------
    StoreSummaryRecord
        One record per CSV row across every discovered file.

    Raises
    ------
    ReconciliationError
        When ``output/daily/`` does not exist, when no date directories
        are found, or when a walked date directory does not contain
        ``store_summary.csv``.
    SchemaValidationError
        When a ``store_summary.csv`` is missing a required column.
    """
    daily_root = Path(root) / "daily"
    if not daily_root.is_dir():
        raise ReconciliationError(
            "sim engine output has no daily/ subtree",
            path=str(daily_root),
        )

    date_dirs = sorted(
        p for p in daily_root.glob("??/??/????") if p.is_dir()
    )
    if not date_dirs:
        raise ReconciliationError(
            "no date directories found under daily/",
            path=str(daily_root),
        )

    for date_dir in date_dirs:
        csv_path = date_dir / "store_summary.csv"
        if not csv_path.is_file():
            raise ReconciliationError(
                "walked date directory is missing store_summary.csv",
                path=str(date_dir),
            )
        yield from _read_store_summary(csv_path)


def load_dim_stores(root: Path) -> pd.DataFrame:
    """Read ``{root}/dimensions/dim_stores.csv`` into a DataFrame.

    Only ``store_id`` is contractually required (and coerced to ``int``);
    other columns pass through unchanged so they remain available to any
    future caller without schema changes here.

    Raises
    ------
    SchemaValidationError
        When the file is missing, unreadable, or lacks required columns.
    """
    csv_path = Path(root) / "dimensions" / "dim_stores.csv"
    if not csv_path.is_file():
        raise SchemaValidationError(
            "dim_stores.csv not found",
            path=str(csv_path),
        )

    df = pd.read_csv(csv_path)
    missing = DIM_STORES_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise SchemaValidationError(
            "dim_stores.csv is missing required columns",
            path=str(csv_path),
            missing_columns=sorted(missing),
        )

    df["store_id"] = df["store_id"].astype(int)
    return df


def _read_store_summary(csv_path: Path) -> Iterator[StoreSummaryRecord]:
    """Parse a single store_summary.csv into typed records."""
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = set(reader.fieldnames or [])
        missing = STORE_SUMMARY_REQUIRED_COLUMNS - fieldnames
        if missing:
            raise SchemaValidationError(
                "store_summary.csv is missing required columns",
                path=str(csv_path),
                missing_columns=sorted(missing),
            )

        source_path = str(csv_path)
        for row in reader:
            try:
                yield StoreSummaryRecord(
                    date=date.fromisoformat(row["date_key"]),
                    store_id=int(row["store_id"]),
                    net_sales_total=float(row["net_sales_total"]),
                    transactions_total=int(row["transactions_total"]),
                    labor_cost_pct=_parse_optional_float(row["labor_cost_pct"]),
                    source_path=source_path,
                )
            except (ValueError, KeyError) as exc:
                raise SchemaValidationError(
                    "unparseable row in store_summary.csv",
                    path=source_path,
                    row=row,
                    cause=str(exc),
                ) from exc


def _parse_optional_float(raw: str) -> float:
    """Parse a CSV cell as float, treating blank/"nan" as NaN."""
    if raw is None:
        return float("nan")
    stripped = raw.strip()
    if not stripped or stripped.lower() == "nan":
        return float("nan")
    return float(stripped)
