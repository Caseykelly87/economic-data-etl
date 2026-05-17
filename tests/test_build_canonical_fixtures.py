"""Tests for scripts/build_canonical_fixtures.py.

The script wraps sim_cli + detect_cli. The test verifies it produces
both parquets at the expected paths when run against a sim engine
output tree. The 24-row happy fixture is used as input.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
HAPPY_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "sim_engine" / "happy" / "output"
SCRIPT = REPO_ROOT / "scripts" / "build_canonical_fixtures.py"


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
