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

import structlog

from src.exceptions import ReconciliationError, SimIngestError
from src.observability import configure_logging
from src.sim_ingest import load_dim_stores, load_store_summaries
from src.sim_transform import build_store_daily_metrics

OUTPUT_FILENAME = "store_daily_metrics.parquet"

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
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser


def run(input_root: Path, output_dir: Path) -> Path:
    """Execute the full ingest pipeline and return the path to the parquet file."""
    log.info("Loading dim_stores from %s", input_root)
    dim_stores = load_dim_stores(input_root)

    log.info("Walking store_summary tree under %s", input_root)
    records = list(load_store_summaries(input_root))
    log.info("Collected %d store-day records", len(records))

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
    log.info("Wrote %d rows to %s", len(df), output_path)
    return output_path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verbose:
        os.environ["LOG_LEVEL"] = "debug"
    configure_logging()

    try:
        run(args.input_root, args.output_dir)
    except SimIngestError as exc:
        log.error("sim ingestion failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
