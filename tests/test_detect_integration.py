"""End-to-end integration: sim_cli -> detect_cli on real fixture trees.

These tests prove the full chain (ingest the sim engine output, then
evaluate the rules and write the flags parquet) cooperates correctly
across happy and anomalous fixtures, and produces byte-identical
output on repeat runs.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
import pytest

from src import detect_cli, sim_cli
from src.schemas import ANOMALY_FLAG_COLUMNS


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def rules_path(request):
    return Path(request.config.rootdir) / "config" / "detection_rules.yaml"


def _full_pipeline(input_root: Path, tmp_path: Path, rules_path: Path) -> Path:
    metrics_dir = tmp_path / "metrics"
    flags_dir = tmp_path / "flags"
    sim_cli.main(["--input-root", str(input_root), "--output-dir", str(metrics_dir)])
    detect_cli.main([
        "--metrics-path", str(metrics_dir / sim_cli.OUTPUT_FILENAME),
        "--sim-output-root", str(input_root),
        "--rules-path", str(rules_path),
        "--output-dir", str(flags_dir),
    ])
    return flags_dir / detect_cli.OUTPUT_FILENAME


# ==============================================================================
# Happy path
# ==============================================================================


def test_happy_path_end_to_end_zero_flags(tmp_path, sim_happy_root, rules_path):
    flags_path = _full_pipeline(sim_happy_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    assert len(df) == 0


# ==============================================================================
# Anomalous path
# ==============================================================================


def test_anomalous_path_end_to_end_expected_flags(
    tmp_path, sim_anomalous_root, rules_path
):
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)

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
    assert expected.issubset(triples)


def test_anomalous_path_no_flags_outside_06_16(
    tmp_path, sim_anomalous_root, rules_path
):
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    other = df[~df["date"].astype(str).eq("2024-06-16")]
    assert len(other) == 0


# ==============================================================================
# Idempotency and schema integrity
# ==============================================================================


def test_full_pipeline_repeat_runs_byte_identical(
    tmp_path, sim_anomalous_root, rules_path
):
    a = _full_pipeline(sim_anomalous_root, tmp_path / "run_a", rules_path)
    b = _full_pipeline(sim_anomalous_root, tmp_path / "run_b", rules_path)
    assert _sha256(a) == _sha256(b)


def test_flags_parquet_columns_match_schema(
    tmp_path, sim_anomalous_root, rules_path
):
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    assert tuple(df.columns) == ANOMALY_FLAG_COLUMNS


def test_flags_parquet_dtypes_round_trip(
    tmp_path, sim_anomalous_root, rules_path
):
    """Numeric columns float64, store_id int64, rule_id and severity_level strings."""
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    assert pd.api.types.is_integer_dtype(df["store_id"])
    for col in ("actual_value", "expected_low", "expected_high",
                "distance_from_band", "severity_score"):
        assert pd.api.types.is_float_dtype(df[col])
    assert df["rule_id"].dtype == object or pd.api.types.is_string_dtype(df["rule_id"])
    assert df["severity_level"].dtype == object or pd.api.types.is_string_dtype(df["severity_level"])


def test_flags_parquet_sorted_by_date_store_rule(
    tmp_path, sim_anomalous_root, rules_path
):
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    triples = list(zip(df["date"], df["store_id"], df["rule_id"]))
    assert triples == sorted(triples)
