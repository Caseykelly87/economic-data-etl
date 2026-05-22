"""Command-line entry point for exception detection.

Reads ``store_daily_metrics.parquet`` (phase 1 output) plus the store
reference data, evaluates the detection rules from
``config/detection_rules.yaml``, and writes ``anomaly_flags.parquet``
with one row per fired rule. Repeat invocations against identical input
produce a byte-identical parquet file.

The ``dim_stores`` reference data is read either from a sim engine
output tree (``--sim-output-root``) or from a committed
``dim_stores.parquet`` (``--dim-stores-path``); exactly one is required.
The parquet form lets detection re-run against the committed canonical
artifacts without the upstream sim engine output on hand.

When ``--department-metrics-path`` is supplied the structural
``department_coverage`` rule additionally evaluates the department-grain
frame; without it that rule is skipped.

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
from src.schemas import DEPARTMENT_DAILY_METRICS_COLUMNS, STORE_DAILY_METRICS_COLUMNS
from src.sim_ingest import load_dim_stores

# Columns run_all_rules dereferences off dim_stores. Narrower than the
# full dim_stores schema on purpose: detection does not need the rest.
DIM_STORES_DETECTION_COLUMNS: tuple[str, ...] = (
    "store_id",
    "base_daily_revenue",
    "trade_area_profile",
)

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
        "--department-metrics-path",
        type=Path,
        default=None,
        help=(
            "Path to department_daily_metrics.parquet. When supplied, the "
            "department_coverage structural rule is evaluated; when omitted "
            "that rule is skipped."
        ),
    )
    dim_stores_source = parser.add_mutually_exclusive_group(required=True)
    dim_stores_source.add_argument(
        "--sim-output-root",
        type=Path,
        help=(
            "Path to the sim engine's output/ directory; dim_stores is read "
            "from its dimensions/dim_stores.csv."
        ),
    )
    dim_stores_source.add_argument(
        "--dim-stores-path",
        type=Path,
        help=(
            "Path to a committed dim_stores.parquet, as an alternative to "
            "--sim-output-root."
        ),
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


def _read_checked_parquet(
    path: Path, required_columns: tuple[str, ...], label: str,
) -> pd.DataFrame:
    """Read a parquet file for detection input.

    Raises :class:`DetectionInputError` when the file is missing,
    unreadable, or lacks a column detection needs. ``label`` names the
    input in error messages (``"metrics"``, ``"dim_stores"``,
    ``"department metrics"``).
    """
    if not path.is_file():
        raise DetectionInputError(f"{label} parquet not found", path=str(path))
    try:
        df = pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise DetectionInputError(
            f"{label} parquet unreadable", path=str(path), cause=str(exc),
        ) from exc
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise DetectionInputError(
            f"{label} parquet is missing required columns",
            path=str(path),
            missing_columns=missing,
        )
    return df


def run(
    metrics_path: Path,
    rules_path: Path,
    output_dir: Path,
    *,
    sim_output_root: Path | None = None,
    dim_stores_path: Path | None = None,
    department_metrics_path: Path | None = None,
) -> Path:
    log.info("loading_rules_config", rules_path=str(rules_path))
    rules_config = load_rules_config(rules_path)

    if dim_stores_path is not None:
        log.info("loading_dim_stores", dim_stores_path=str(dim_stores_path))
        dim_stores = _read_checked_parquet(
            dim_stores_path, DIM_STORES_DETECTION_COLUMNS, "dim_stores",
        )
    else:
        log.info("loading_dim_stores", sim_output_root=str(sim_output_root))
        try:
            dim_stores = load_dim_stores(sim_output_root)
        except SchemaValidationError as exc:
            raise DetectionInputError(
                "dim_stores load failed",
                path=str(sim_output_root), cause=str(exc),
            ) from exc

    log.info("reading_metrics_parquet", metrics_path=str(metrics_path))
    metrics_df = _read_checked_parquet(
        metrics_path, STORE_DAILY_METRICS_COLUMNS, "metrics",
    )
    log.info("metrics_loaded", row_count=len(metrics_df))

    department_metrics_df = None
    if department_metrics_path is not None:
        log.info(
            "reading_department_metrics_parquet",
            department_metrics_path=str(department_metrics_path),
        )
        department_metrics_df = _read_checked_parquet(
            department_metrics_path,
            DEPARTMENT_DAILY_METRICS_COLUMNS,
            "department metrics",
        )
        log.info(
            "department_metrics_loaded", row_count=len(department_metrics_df),
        )

    flags = run_all_rules(
        metrics_df, dim_stores, rules_config,
        department_metrics_df=department_metrics_df,
    )

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
        run(
            args.metrics_path,
            args.rules_path,
            args.output_dir,
            sim_output_root=args.sim_output_root,
            dim_stores_path=args.dim_stores_path,
            department_metrics_path=args.department_metrics_path,
        )
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
