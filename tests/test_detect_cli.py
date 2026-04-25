"""CLI-layer tests for src.detect_cli.

Exercises the runnable entry point against the on-disk fixtures: happy
path produces zero flags, anomalous fixture produces the expected flag
set, repeat invocations are byte-identical, and each typed failure
mode (missing input, malformed YAML, schema-broken parquet) returns
exit code 1.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from src import detect_cli, sim_cli
from src.schemas import ANOMALY_FLAG_COLUMNS


# ----- helpers ----------------------------------------------------------------


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def rules_path(request):
    return Path(request.config.rootdir) / "config" / "detection_rules.yaml"


def _ingest(input_root: Path, output_dir: Path) -> Path:
    sim_cli.main(["--input-root", str(input_root), "--output-dir", str(output_dir)])
    return output_dir / sim_cli.OUTPUT_FILENAME


# ==============================================================================
# Happy path
# ==============================================================================


def test_cli_writes_parquet_on_happy_path(tmp_path, sim_happy_root, rules_path):
    metrics_path = _ingest(sim_happy_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 0
    assert (out / detect_cli.OUTPUT_FILENAME).is_file()


def test_cli_happy_fixture_produces_zero_flags(tmp_path, sim_happy_root, rules_path):
    metrics_path = _ingest(sim_happy_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)
    assert len(df) == 0


def test_cli_parquet_has_anomaly_flag_schema(tmp_path, sim_happy_root, rules_path):
    metrics_path = _ingest(sim_happy_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)
    assert tuple(df.columns) == ANOMALY_FLAG_COLUMNS


# ==============================================================================
# Anomalous path
# ==============================================================================


def test_cli_anomalous_fixture_fires_expected_rules(
    tmp_path, sim_anomalous_root, rules_path
):
    """The fixture's deliberate injections must each surface a flag.

    See tests/fixtures/sim_engine/anomalous/README.md for the per-row
    rationale; the asserts below pin the expected (date, store_id,
    rule_id) triples without locking severity bucket — severity is
    re-tested at unit grain in test_detect_rules.py.
    """
    metrics_path = _ingest(sim_anomalous_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_anomalous_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)

    triples = set(zip(
        df["date"].astype(str),
        df["store_id"].astype(int),
        df["rule_id"].astype(str),
    ))
    expected = {
        ("2024-06-16", 1, "revenue_band"),
        ("2024-06-16", 1, "transactions_band"),
        ("2024-06-16", 4, "labor_pct_band"),
        ("2024-06-16", 7, "transactions_band"),
        ("2024-06-16", 7, "avg_ticket_band"),
    }
    assert expected.issubset(triples), (
        f"missing expected flags: {expected - triples}"
    )


def test_cli_anomalous_fixture_no_flags_on_normal_dates(
    tmp_path, sim_anomalous_root, rules_path
):
    metrics_path = _ingest(sim_anomalous_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_anomalous_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)
    normal_dates = df[df["date"].astype(str).isin(["2024-06-15", "2024-06-17"])]
    assert len(normal_dates) == 0


def test_cli_repeat_runs_byte_identical(tmp_path, sim_anomalous_root, rules_path):
    metrics_path = _ingest(sim_anomalous_root, tmp_path / "metrics")
    out_a = tmp_path / "flags_a"
    out_b = tmp_path / "flags_b"
    for out in (out_a, out_b):
        exit_code = detect_cli.main([
            "--metrics-path", str(metrics_path),
            "--sim-output-root", str(sim_anomalous_root),
            "--rules-path", str(rules_path),
            "--output-dir", str(out),
        ])
        assert exit_code == 0
    assert _sha256(out_a / detect_cli.OUTPUT_FILENAME) == _sha256(
        out_b / detect_cli.OUTPUT_FILENAME
    )


# ==============================================================================
# Failure modes
# ==============================================================================


def test_cli_exits_nonzero_on_missing_metrics_file(
    tmp_path, sim_happy_root, rules_path
):
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(tmp_path / "does_not_exist.parquet"),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 1


def test_cli_exits_nonzero_on_malformed_yaml(
    tmp_path, sim_happy_root
):
    bad = tmp_path / "broken.yaml"
    bad.write_text("severity:\n  info_max: 1.0\n", encoding="utf-8")  # missing rules:
    metrics_path = _ingest(sim_happy_root, tmp_path / "metrics")
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(bad),
        "--output-dir", str(out),
    ])
    assert exit_code == 1


def test_cli_exits_nonzero_on_metrics_missing_required_column(
    tmp_path, sim_happy_root, rules_path
):
    """A parquet missing labor_cost_pct must not be silently accepted."""
    metrics_path = _ingest(sim_happy_root, tmp_path / "metrics")
    df = pd.read_parquet(metrics_path)
    df.drop(columns=["labor_cost_pct"]).to_parquet(metrics_path, index=False)

    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--sim-output-root", str(sim_happy_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 1
