"""Build canonical parquet fixtures from a sim engine output tree.

This developer-facing script orchestrates the existing sim_cli and
detect_cli modules to produce store_daily_metrics.parquet,
department_daily_metrics.parquet, and anomaly_flags.parquet from a
sim engine output directory. Used to regenerate the committed
canonical fixtures at data/processed/canonical/ when the underlying
sim engine output changes.

NOT collected by pytest (lives in scripts/, not tests/). Intended
for manual developer invocation.

Usage
-----
    python scripts/build_canonical_fixtures.py \\
        --sim-output-root /path/to/sim/engine/output \\
        --output-dir data/processed/canonical/

Exit codes
----------
0
    All three parquets written successfully.
nonzero
    The wrapped sim_cli or detect_cli failed; their stderr is
    surfaced to this script's stderr.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="build_canonical_fixtures",
        description=(
            "Run sim_cli + detect_cli against a sim engine output tree "
            "and write the resulting canonical parquets to --output-dir."
        ),
    )
    parser.add_argument(
        "--sim-output-root",
        type=Path,
        required=True,
        help=(
            "Path to the sim engine's output/ directory. Must contain "
            "dimensions/dim_stores.csv and a populated daily/{MM}/{DD}/{YYYY}/ tree."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write store_daily_metrics.parquet and anomaly_flags.parquet into.",
    )
    parser.add_argument(
        "--rules-path",
        type=Path,
        default=Path("config/detection_rules.yaml"),
        help="Path to the detection rules config (default: config/detection_rules.yaml).",
    )
    return parser


def _validate_input_root(sim_output_root: Path) -> None:
    """Fail fast if the sim engine output looks wrong."""
    if not sim_output_root.is_dir():
        log.error("Sim engine output root does not exist: %s", sim_output_root)
        sys.exit(2)
    dim_stores = sim_output_root / "dimensions" / "dim_stores.csv"
    if not dim_stores.is_file():
        log.error("Expected dim_stores.csv at %s - not found", dim_stores)
        sys.exit(2)
    daily = sim_output_root / "daily"
    if not daily.is_dir():
        log.error("Expected daily/ subdirectory at %s - not found", daily)
        sys.exit(2)


def run(sim_output_root: Path, output_dir: Path, rules_path: Path) -> None:
    """Orchestrate sim_cli followed by detect_cli."""
    _validate_input_root(sim_output_root)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(
        "Step 1/2: invoking sim_cli to produce store_daily_metrics.parquet "
        "and department_daily_metrics.parquet"
    )
    sim_result = subprocess.run(
        [
            sys.executable, "-m", "src.sim_cli",
            "--input-root", str(sim_output_root),
            "--output-dir", str(output_dir),
        ],
        capture_output=True,
        text=True,
    )
    if sim_result.returncode != 0:
        log.error("sim_cli failed (exit %d)", sim_result.returncode)
        log.error("sim_cli stderr:\n%s", sim_result.stderr)
        sys.exit(sim_result.returncode)
    log.info("sim_cli output:\n%s", sim_result.stdout.strip())

    metrics_path = output_dir / "store_daily_metrics.parquet"
    if not metrics_path.is_file():
        log.error("sim_cli reported success but %s does not exist", metrics_path)
        sys.exit(3)

    department_metrics_path = output_dir / "department_daily_metrics.parquet"
    if not department_metrics_path.is_file():
        log.error(
            "sim_cli reported success but %s does not exist",
            department_metrics_path,
        )
        sys.exit(3)

    log.info("Step 2/2: invoking detect_cli to produce anomaly_flags.parquet")
    detect_result = subprocess.run(
        [
            sys.executable, "-m", "src.detect_cli",
            "--metrics-path", str(metrics_path),
            "--sim-output-root", str(sim_output_root),
            "--rules-path", str(rules_path),
            "--output-dir", str(output_dir),
        ],
        capture_output=True,
        text=True,
    )
    if detect_result.returncode != 0:
        log.error("detect_cli failed (exit %d)", detect_result.returncode)
        log.error("detect_cli stderr:\n%s", detect_result.stderr)
        sys.exit(detect_result.returncode)
    log.info("detect_cli output:\n%s", detect_result.stdout.strip())

    flags_path = output_dir / "anomaly_flags.parquet"
    if not flags_path.is_file():
        log.error("detect_cli reported success but %s does not exist", flags_path)
        sys.exit(3)

    log.info("Canonical fixtures written:")
    log.info("  %s", metrics_path)
    log.info("  %s", department_metrics_path)
    log.info("  %s", flags_path)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run(
        sim_output_root=args.sim_output_root.resolve(),
        output_dir=args.output_dir.resolve(),
        rules_path=args.rules_path.resolve(),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
