"""CLI-layer tests for src.sim_cli.

Exercises the runnable entry point against the on-disk fixtures:
happy-path parquet write, idempotent repeat runs (byte-identical),
and non-zero exit codes on corrupt and partial inputs.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd

from src import sim_cli
from src.schemas import STORE_DAILY_METRICS_COLUMNS


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_cli_writes_parquet_on_happy_path(tmp_path, sim_happy_root):
    exit_code = sim_cli.main(
        [
            "--input-root",
            str(sim_happy_root),
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert exit_code == 0
    assert (tmp_path / sim_cli.OUTPUT_FILENAME).is_file()


def test_cli_parquet_has_target_schema(tmp_path, sim_happy_root):
    sim_cli.main(
        [
            "--input-root",
            str(sim_happy_root),
            "--output-dir",
            str(tmp_path),
        ]
    )
    df = pd.read_parquet(tmp_path / sim_cli.OUTPUT_FILENAME)
    assert tuple(df.columns) == STORE_DAILY_METRICS_COLUMNS
    assert len(df) == 24


def test_cli_repeat_runs_are_byte_identical(tmp_path, sim_happy_root):
    """Full-rebuild semantics: identical input → identical parquet bytes."""
    first = tmp_path / "run_1"
    second = tmp_path / "run_2"

    assert (
        sim_cli.main(
            ["--input-root", str(sim_happy_root), "--output-dir", str(first)]
        )
        == 0
    )
    assert (
        sim_cli.main(
            ["--input-root", str(sim_happy_root), "--output-dir", str(second)]
        )
        == 0
    )

    assert _sha256(first / sim_cli.OUTPUT_FILENAME) == _sha256(
        second / sim_cli.OUTPUT_FILENAME
    )


def test_cli_exits_nonzero_on_corrupt_input(tmp_path, sim_corrupt_root):
    exit_code = sim_cli.main(
        [
            "--input-root",
            str(sim_corrupt_root),
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert exit_code == 1


def test_cli_exits_nonzero_on_partial_input(tmp_path, sim_partial_root):
    exit_code = sim_cli.main(
        [
            "--input-root",
            str(sim_partial_root),
            "--output-dir",
            str(tmp_path),
        ]
    )
    assert exit_code == 1


def test_cli_overwrites_existing_output(tmp_path, sim_happy_root):
    """A second run in the same directory replaces, not appends to, the file."""
    sim_cli.main(
        ["--input-root", str(sim_happy_root), "--output-dir", str(tmp_path)]
    )
    first_hash = _sha256(tmp_path / sim_cli.OUTPUT_FILENAME)

    sim_cli.main(
        ["--input-root", str(sim_happy_root), "--output-dir", str(tmp_path)]
    )
    second_hash = _sha256(tmp_path / sim_cli.OUTPUT_FILENAME)

    assert first_hash == second_hash
