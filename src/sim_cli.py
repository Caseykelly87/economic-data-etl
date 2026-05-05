"""Command-line entry point for sim engine ingestion.

Composes the source adapter and the transform, enforces a final
row-count reconciliation, and writes a deterministic
``store_daily_metrics.parquet`` via pyarrow. Repeat invocations against
identical input produce byte-identical output.

Exit codes
----------
0
    Success.
1
    A :class:`SimIngestError` was raised (schema or reconciliation failure).
    Unexpected exceptions propagate with Python's default nonzero exit.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import structlog

from src.exceptions import ReconciliationError, SimIngestError
from src.observability import configure_logging
from src.schemas import DIM_STORES_FULL_COLUMNS
from src.sim_ingest import (
    load_department_sales,
    load_dim_stores,
    load_store_summaries,
)
from src.sim_transform import (
    build_department_daily_metrics,
    build_store_daily_metrics,
)

OUTPUT_FILENAME = "store_daily_metrics.parquet"
DEPARTMENT_OUTPUT_FILENAME = "department_daily_metrics.parquet"
DIM_STORES_OUTPUT_FILENAME = "dim_stores.parquet"

log = structlog.get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sim_cli",
        description=(
            "Ingest the sim engine's daily output tree and write "
            "store_daily_metrics.parquet."
        ),
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        required=True,
        help="Path to the sim engine's output/ directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write store_daily_metrics.parquet into.",
    )
    parser.add_argument(
        "--no-departments",
        action="store_true",
        help=(
            "Skip ingestion of department_sales.csv files. Only "
            "store_daily_metrics.parquet is written. Useful for fast "
            "tests; rare in production."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser


def run(input_root: Path, output_dir: Path) -> Path:
    """Execute the full ingest pipeline and return the path to the parquet file."""
    log.info("loading_dim_stores", input_root=str(input_root))
    dim_stores = load_dim_stores(input_root)

    log.info("walking_store_summary_tree", input_root=str(input_root))
    records = list(load_store_summaries(input_root))
    log.info("ingestion_records_collected", record_count=len(records))

    df = build_store_daily_metrics(records, dim_stores)

    if len(df) != len(records):
        raise ReconciliationError(
            "output row count does not equal input row count",
            input_rows=len(records),
            output_rows=len(df),
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILENAME
    df.to_parquet(output_path, engine="pyarrow", index=False)
    log.info("parquet_written", row_count=len(df), output_path=str(output_path))
    return output_path


def _write_dim_stores_parquet(
    dim_stores: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """Write the dim_stores DataFrame to the canonical parquet output.

    Reorders columns to match :data:`DIM_STORES_FULL_COLUMNS` and sorts
    rows by ``store_id`` so repeat invocations against identical input
    produce byte-identical output regardless of the source CSV's column
    or row ordering.
    """
    df = dim_stores[list(DIM_STORES_FULL_COLUMNS)].copy()
    df = df.sort_values(by="store_id", kind="mergesort").reset_index(drop=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / DIM_STORES_OUTPUT_FILENAME
    df.to_parquet(output_path, engine="pyarrow", index=False)
    return output_path


def _run_department_grain(input_root: Path, output_dir: Path) -> Path:
    """Execute the department-grain ingest and return the parquet path.

    Mirrors :func:`run` for the store-day grain. Loads dim_stores again
    (cheap: an 8-row CSV) so the helper is self-contained and matches
    the existing reconciliation pattern: row count of the output frame
    must equal the count of input records.
    """
    log.info("loading_dim_stores", input_root=str(input_root), grain="store_day_department")
    dim_stores = load_dim_stores(input_root)

    log.info("walking_department_sales_tree", input_root=str(input_root))
    records = list(load_department_sales(input_root))
    log.info(
        "ingestion_records_collected",
        record_count=len(records),
        grain="store_day_department",
    )

    df = build_department_daily_metrics(records, dim_stores)

    if len(df) != len(records):
        raise ReconciliationError(
            "department output row count does not equal input row count",
            input_rows=len(records),
            output_rows=len(df),
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / DEPARTMENT_OUTPUT_FILENAME
    df.to_parquet(output_path, engine="pyarrow", index=False)
    log.info(
        "parquet_written",
        row_count=len(df),
        output_path=str(output_path),
        grain="store_day_department",
    )
    return output_path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verbose:
        os.environ["LOG_LEVEL"] = "debug"
    configure_logging()

    try:
        run(args.input_root, args.output_dir)

        log.info(
            "loading_dim_stores",
            input_root=str(args.input_root),
            artifact="dim_stores",
        )
        dim_stores = load_dim_stores(args.input_root)
        dim_stores_path = _write_dim_stores_parquet(dim_stores, args.output_dir)
        log.info(
            "parquet_written",
            row_count=len(dim_stores),
            output_path=str(dim_stores_path),
            artifact="dim_stores",
        )

        if not args.no_departments:
            _run_department_grain(args.input_root, args.output_dir)
    except SimIngestError as exc:
        log.error(
            "sim_ingestion_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
