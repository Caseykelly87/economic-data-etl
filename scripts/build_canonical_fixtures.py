"""Build canonical parquet fixtures from a sim engine output tree.

This developer-facing script orchestrates the existing sim_cli and
detect_cli modules to produce store_daily_metrics.parquet,
department_daily_metrics.parquet, and anomaly_flags.parquet from a
sim engine output directory. Used to regenerate the committed
canonical fixtures at data/processed/canonical/ when the underlying
sim engine output changes.

It also invokes evaluate_detection.py to write detection_quality.json
alongside the parquets, capturing recall, FPR, per-anomaly-type
recall, and the phase 2 contract verdict in a form downstream
consumers (API endpoint, portal page) can read directly.

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
    All parquets written successfully. The detection_quality.json
    artifact is written when a sim engine anomaly_log.csv tree is
    present; a failing contract verdict is logged as a warning but
    does not fail the build.
nonzero
    The wrapped sim_cli or detect_cli failed, or evaluate_detection
    failed in a way that prevented the JSON artifact from being
    written; their stderr is surfaced to this script's stderr.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import tempfile
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
            "dimensions/dim_stores.csv and a populated daily/{YYYY}/{MM}/{DD}/ tree."
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
        log.error("sim engine output root does not exist: %s", sim_output_root)
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
        "Step 1/2: invoking sim_cli to produce store_daily_metrics.parquet, "
        "department_daily_metrics.parquet, and dim_stores.parquet"
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

    dim_stores_path = output_dir / "dim_stores.parquet"
    if not dim_stores_path.is_file():
        log.error(
            "sim_cli reported success but %s does not exist",
            dim_stores_path,
        )
        sys.exit(3)

    log.info("Step 2/2: invoking detect_cli to produce anomaly_flags.parquet")
    detect_result = subprocess.run(
        [
            sys.executable, "-m", "src.detect_cli",
            "--metrics-path", str(metrics_path),
            "--department-metrics-path", str(department_metrics_path),
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

    log.info(
        "Step 3/3: invoking evaluate_detection to produce detection_quality.json"
    )
    detection_quality_path = _run_evaluate_detection(
        sim_output_root=sim_output_root,
        metrics_path=metrics_path,
        flags_path=flags_path,
        output_dir=output_dir,
    )

    log.info("Canonical fixtures written:")
    log.info("  %s", metrics_path)
    log.info("  %s", department_metrics_path)
    log.info("  %s", dim_stores_path)
    log.info("  %s", flags_path)
    if detection_quality_path is not None:
        log.info("  %s", detection_quality_path)


def _run_evaluate_detection(
    sim_output_root: Path,
    metrics_path: Path,
    flags_path: Path,
    output_dir: Path,
) -> Path | None:
    """Aggregate the sim engine's daily anomaly_log.csv files and run
    evaluate_detection.py, writing detection_quality.json into output_dir.

    Returns the JSON path on success (including the contract-fail case,
    which is logged as a warning but not treated as a build error).
    Returns None when the sim output tree contains no daily anomaly_log
    files — typical of the minimal test fixtures, which inject no
    anomalies and therefore have nothing to measure recall against.
    Exits non-zero only when evaluate_detection itself fails to produce
    the JSON artifact.
    """
    daily_logs = sorted(sim_output_root.glob("daily/*/*/*/anomaly_log.csv"))
    if not daily_logs:
        log.info(
            "No daily anomaly_log.csv files under %s/daily/ - "
            "skipping detection-quality measurement.",
            sim_output_root,
        )
        return None

    log.info(
        "Aggregating %d daily anomaly_log.csv files for evaluate_detection",
        len(daily_logs),
    )

    detection_quality_path = output_dir / "detection_quality.json"
    repo_root = Path(__file__).resolve().parent.parent
    evaluate_script = repo_root / "scripts" / "evaluate_detection.py"

    with tempfile.TemporaryDirectory() as tmpdir:
        aggregated_log = Path(tmpdir) / "anomaly_log.csv"
        _concatenate_csvs(daily_logs, aggregated_log)

        eval_result = subprocess.run(
            [
                sys.executable, str(evaluate_script),
                "--flags-path", str(flags_path),
                "--metrics-path", str(metrics_path),
                "--anomaly-log-path", str(aggregated_log),
                "--output-path", str(detection_quality_path),
            ],
            capture_output=True,
            text=True,
        )

    if not detection_quality_path.is_file():
        log.error(
            "evaluate_detection failed (exit %d) and did not write %s",
            eval_result.returncode,
            detection_quality_path,
        )
        log.error("evaluate_detection stderr:\n%s", eval_result.stderr)
        sys.exit(eval_result.returncode or 3)

    log.info("evaluate_detection output:\n%s", eval_result.stdout.strip())

    # Contract verdict failure is exit code 1 with the JSON still written.
    # Surface as a warning; the artifact is information for downstream
    # consumers, not a gate on the canonical build.
    if eval_result.returncode != 0:
        log.warning(
            "Detection contract verdict: FAIL (evaluate_detection exit %d). "
            "detection_quality.json was written; downstream consumers will "
            "render the failing verdict.",
            eval_result.returncode,
        )

    return detection_quality_path


def _concatenate_csvs(sources: list[Path], destination: Path) -> None:
    """Concatenate sources into destination with a single header row.

    Avoids a pandas dependency in this helper so the orchestration
    stays import-light; the per-day anomaly_log files share an
    identical five-column header by sim engine convention.
    """
    with destination.open("w", encoding="utf-8", newline="") as out:
        header_written = False
        for src in sources:
            with src.open("r", encoding="utf-8", newline="") as fh:
                header = fh.readline()
                if not header_written:
                    out.write(header)
                    header_written = True
                for line in fh:
                    if line.strip():
                        out.write(line)


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
