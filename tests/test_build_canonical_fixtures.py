"""Tests for scripts/build_canonical_fixtures.py.

The script wraps sim_cli + detect_cli, then invokes evaluate_detection
to write detection_quality.json. These tests verify both that the
parquets land at the expected paths when run against a sim engine
output tree (the 24-row happy fixture is used as input) and that the
detection_quality.json shape the downstream API and portal depend on
stays pinned.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
HAPPY_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "sim_engine" / "happy" / "output"
SCRIPT = REPO_ROOT / "scripts" / "build_canonical_fixtures.py"
EVALUATE_SCRIPT = REPO_ROOT / "scripts" / "evaluate_detection.py"
CANONICAL_DIR = REPO_ROOT / "data" / "processed" / "canonical"


class TestBuildCanonicalFixtures:
    def test_script_exists(self):
        assert SCRIPT.is_file(), f"Build script not found at {SCRIPT}"

    def test_produces_metrics_parquet(self, tmp_path: Path):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--sim-output-root", str(HAPPY_FIXTURE),
                "--output-dir", str(tmp_path),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, (
            f"Script exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        metrics_path = tmp_path / "store_daily_metrics.parquet"
        assert metrics_path.is_file()

    def test_produces_anomaly_flags_parquet(self, tmp_path: Path):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--sim-output-root", str(HAPPY_FIXTURE),
                "--output-dir", str(tmp_path),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, (
            f"Script exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        flags_path = tmp_path / "anomaly_flags.parquet"
        assert flags_path.is_file()

    def test_produces_dim_stores_parquet(self, tmp_path: Path):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--sim-output-root", str(HAPPY_FIXTURE),
                "--output-dir", str(tmp_path),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, (
            f"Script exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        dim_stores_path = tmp_path / "dim_stores.parquet"
        assert dim_stores_path.is_file()
        df = pd.read_parquet(dim_stores_path)
        assert len(df) == 8

    def test_metrics_parquet_has_expected_shape(self, tmp_path: Path):
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--sim-output-root", str(HAPPY_FIXTURE),
                "--output-dir", str(tmp_path),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            check=True,
        )
        df = pd.read_parquet(tmp_path / "store_daily_metrics.parquet")
        # The happy fixture has 24 rows (3 dates x 8 stores)
        assert len(df) == 24
        # Column count and key columns confirmed by Phase 2 schema
        assert "date" in df.columns
        assert "store_id" in df.columns
        assert "total_sales" in df.columns
        assert "labor_cost_pct" in df.columns

    def test_byte_identical_repeat_runs(self, tmp_path: Path):
        """Running the script twice against the same input produces
        byte-identical parquets. The committed canonical fixtures depend
        on this property."""
        out_a = tmp_path / "run_a"
        out_b = tmp_path / "run_b"
        for out in (out_a, out_b):
            subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--sim-output-root", str(HAPPY_FIXTURE),
                    "--output-dir", str(out),
                ],
                capture_output=True,
                text=True,
                cwd=str(REPO_ROOT),
                check=True,
            )
        assert (out_a / "store_daily_metrics.parquet").read_bytes() == (
            out_b / "store_daily_metrics.parquet"
        ).read_bytes()
        assert (out_a / "anomaly_flags.parquet").read_bytes() == (
            out_b / "anomaly_flags.parquet"
        ).read_bytes()

    def test_nonexistent_input_root_exits_nonzero(self, tmp_path: Path):
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--sim-output-root", str(tmp_path / "does_not_exist"),
                "--output-dir", str(tmp_path / "out"),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        assert result.returncode != 0


class TestDetectionQualityJsonShape:
    """Pin the detection_quality.json shape contract.

    Structural: the API endpoint and portal page in the downstream
    repos parse this JSON by key. Changing the shape silently would
    break them. Running evaluate_detection against the committed
    canonical parquets and asserting key presence keeps the contract
    visible in this repo's test suite.
    """

    def _synthetic_anomaly_log(
        self, metrics_path: Path, destination: Path
    ) -> None:
        """Write a tiny anomaly_log.csv whose (date, store_id) pairs
        are real rows from the metrics parquet. Keeps the test
        independent of any external sim engine output tree while
        still exercising the full evaluate_detection code path."""
        metrics = pd.read_parquet(metrics_path)
        sample = metrics[["date", "store_id"]].head(3).copy()
        sample["date_key"] = sample["date"].astype(str)
        sample["department_id"] = 1
        sample["anomaly_type"] = "missing_department"
        sample["description"] = "synthetic test row"
        sample[
            ["date_key", "store_id", "department_id",
             "anomaly_type", "description"]
        ].to_csv(destination, index=False)

    def test_evaluate_detection_writes_expected_shape(self, tmp_path: Path):
        """Structural: detection_quality.json carries the keys the API
        and portal read. If this fails, downstream consumers break."""
        flags = CANONICAL_DIR / "anomaly_flags.parquet"
        metrics = CANONICAL_DIR / "store_daily_metrics.parquet"
        if not flags.is_file() or not metrics.is_file():
            import pytest
            pytest.skip("canonical parquets not present")

        anomaly_log = tmp_path / "anomaly_log.csv"
        self._synthetic_anomaly_log(metrics, anomaly_log)

        output_path = tmp_path / "detection_quality.json"
        result = subprocess.run(
            [
                sys.executable, str(EVALUATE_SCRIPT),
                "--flags-path", str(flags),
                "--metrics-path", str(metrics),
                "--anomaly-log-path", str(anomaly_log),
                "--output-path", str(output_path),
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
        )
        # The contract verdict may PASS or FAIL depending on the
        # synthetic log; either is allowed here. What matters is that
        # the JSON exists and has the agreed shape.
        assert output_path.is_file(), (
            f"detection_quality.json was not written.\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        report = json.loads(output_path.read_text(encoding="utf-8"))
        expected_top_level = {
            "global", "by_anomaly_type", "false_positive_rate",
            "false_positives", "negative_universe", "flag_rate",
            "total_flags", "total_metric_rows",
        }
        assert expected_top_level.issubset(report.keys()), (
            f"missing top-level keys: {expected_top_level - set(report.keys())}"
        )

        assert {"injected_pairs", "matched_pairs", "recall"}.issubset(
            report["global"].keys()
        )
        assert isinstance(report["global"]["recall"], (int, float))
        assert isinstance(report["false_positive_rate"], (int, float))
        assert isinstance(report["by_anomaly_type"], dict)
        assert report["by_anomaly_type"], (
            "expected at least one by_anomaly_type entry"
        )
        for atype, stats in report["by_anomaly_type"].items():
            assert {"injected", "matched", "recall"}.issubset(stats.keys()), (
                f"by_anomaly_type[{atype!r}] missing required keys"
            )

    def test_committed_detection_quality_json_parses(self):
        """Structural: the committed artifact in data/processed/canonical
        stays parseable and carries the contract keys downstream consumers
        read. Catches accidental hand-edits or stale commits."""
        committed = CANONICAL_DIR / "detection_quality.json"
        if not committed.is_file():
            import pytest
            pytest.skip("detection_quality.json not present in canonical/")
        report = json.loads(committed.read_text(encoding="utf-8"))
        assert "global" in report and "recall" in report["global"]
        assert "false_positive_rate" in report
        assert isinstance(report["by_anomaly_type"], dict)
