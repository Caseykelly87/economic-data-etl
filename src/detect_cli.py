"""Command-line entry point for exception detection.

Reads ``store_daily_metrics.parquet`` (phase 1 output) plus the
operational reference data in ``dim_stores.csv``, evaluates the five
detection rules from ``config/detection_rules.yaml`` against every
store-day, and writes ``anomaly_flags.parquet`` with one row per fired
rule. Repeat invocations against identical input produce a
byte-identical parquet file.

Exit codes
----------
0
    Success.
1
    A :class:`DetectionError` was raised (config malformed, input
    parquet missing or schema-broken). Unexpected exceptions
    propagate with Python's default nonzero exit, mirroring sim_cli.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import structlog

from src.detect_rules import load_rules_config, run_all_rules
from src.exceptions import (
    DetectionError,
    DetectionInputError,
    SchemaValidationError,
)
from src.observability import configure_logging
from src.schemas import STORE_DAILY_METRICS_COLUMNS
from src.sim_ingest import load_dim_stores

OUTPUT_FILENAME = "anomaly_flags.parquet"

log = structlog.get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="detect_cli",
        description=(
            "Evaluate exception detection rules against the metrics parquet "
            "and write anomaly_flags.parquet."
        ),
    )
    parser.add_argument(
        "--metrics-path",
        type=Path,
        required=True,
        help="Path to store_daily_metrics.parquet produced by sim_cli.",
    )
    parser.add_argument(
        "--sim-output-root",
        type=Path,
        required=True,
        help="Path to the sim engine's output/ directory (for dim_stores).",
    )
    parser.add_argument(
        "--rules-path",
        type=Path,
        default=Path("config") / "detection_rules.yaml",
        help="Path to the detection rules YAML.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write anomaly_flags.parquet into.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser


def run(
    metrics_path: Path,
    sim_output_root: Path,
    rules_path: Path,
    output_dir: Path,
) -> Path:
    log.info("loading_rules_config", rules_path=str(rules_path))
    rules_config = load_rules_config(rules_path)

    log.info("loading_dim_stores", sim_output_root=str(sim_output_root))
    try:
        dim_stores = load_dim_stores(sim_output_root)
    except SchemaValidationError as exc:
        raise DetectionInputError(
            "dim_stores load failed", path=str(sim_output_root), cause=str(exc),
        ) from exc

    log.info("reading_metrics_parquet", metrics_path=str(metrics_path))
    if not metrics_path.is_file():
        raise DetectionInputError(
            "metrics parquet not found", path=str(metrics_path),
        )
    try:
        metrics_df = pd.read_parquet(metrics_path)
    except (OSError, ValueError) as exc:
        raise DetectionInputError(
            "metrics parquet unreadable", path=str(metrics_path), cause=str(exc),
        ) from exc

    missing = [c for c in STORE_DAILY_METRICS_COLUMNS if c not in metrics_df.columns]
    if missing:
        raise DetectionInputError(
            "metrics parquet is missing required columns",
            path=str(metrics_path),
            missing_columns=missing,
        )

    log.info("metrics_loaded", row_count=len(metrics_df))

    flags = run_all_rules(metrics_df, dim_stores, rules_config)

    log.info("detection_complete", flag_count=len(flags))

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILENAME
    flags.to_parquet(output_path, engine="pyarrow", index=False)
    log.info(
        "anomaly_flags_written",
        output_path=str(output_path),
        flag_count=len(flags),
    )
    return output_path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.verbose:
        os.environ["LOG_LEVEL"] = "debug"
    configure_logging()

    try:
        run(args.metrics_path, args.sim_output_root, args.rules_path, args.output_dir)
    except DetectionError as exc:
        log.error(
            "detection_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
