"""CLI-layer tests for src.detect_cli.

Exercises the runnable entry point against the on-disk fixtures: happy
path produces zero flags, anomalous fixture produces the expected flag
set, repeat invocations are byte-identical, and each typed failure
mode (missing input, malformed YAML, schema-broken parquet) returns
exit code 1.
"""

from __future__ import annotations

from datetime import date
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


def _dept_rows(d: date, store_id: int, department_ids: list[int]) -> list[dict]:
    """department_daily_metrics rows for one store-day."""
    return [
        {
            "date": d,
            "store_id": store_id,
            "department_id": dept_id,
            "net_sales": 1000.0,
            "transactions": 50,
            "units_sold": 120,
            "gross_margin_pct": 0.30,
        }
        for dept_id in department_ids
    ]


def _write_detection_inputs(
    tmp_path: Path, department_rows: list[dict],
) -> tuple[Path, Path, Path]:
    """Write minimal store-metrics, dim_stores, and department-metrics
    parquets to ``tmp_path``. The single store-day sits inside every band
    so the only flags that can fire are structural ones. Returns the three
    parquet paths."""
    d = date(2024, 6, 15)
    store_metrics = pd.DataFrame({
        "date": [d],
        "store_id": pd.Series([1], dtype="int64"),
        "total_sales": [95000.0],
        "transaction_count": pd.Series([2500], dtype="int64"),
        "avg_basket_size": [38.0],
        "labor_cost_pct": [0.105],
    })
    dim_stores = pd.DataFrame({
        "store_id": pd.Series([1], dtype="int64"),
        "base_daily_revenue": [95000.0],
        "trade_area_profile": ["suburban-family"],
    })
    metrics_path = tmp_path / "store_daily_metrics.parquet"
    dim_path = tmp_path / "dim_stores.parquet"
    dept_path = tmp_path / "department_daily_metrics.parquet"
    store_metrics.to_parquet(metrics_path, index=False)
    dim_stores.to_parquet(dim_path, index=False)
    pd.DataFrame(department_rows).to_parquet(dept_path, index=False)
    return metrics_path, dim_path, dept_path


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


# ==============================================================================
# Structural rule — department metrics and dim_stores parquet inputs
# ==============================================================================


def test_cli_department_coverage_fires_with_department_metrics(tmp_path, rules_path):
    """--department-metrics-path drives the structural rule; a store-day
    missing a department surfaces a department_coverage flag. The run also
    exercises --dim-stores-path as the dim_stores source."""
    dept_rows = _dept_rows(date(2024, 6, 15), 1, list(range(1, 10)))  # 9 of 10
    metrics_path, dim_path, dept_path = _write_detection_inputs(tmp_path, dept_rows)
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--department-metrics-path", str(dept_path),
        "--dim-stores-path", str(dim_path),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 0
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)
    assert tuple(df.columns) == ANOMALY_FLAG_COLUMNS
    structural = df[df["rule_id"] == "department_coverage"]
    assert len(structural) == 1
    assert structural["actual_value"].iloc[0] == 9.0
    assert structural["severity_level"].iloc[0] == "warning"


def test_cli_skips_structural_rule_without_department_metrics(tmp_path, rules_path):
    """Omitting --department-metrics-path skips the structural rule; the
    --dim-stores-path source still drives the band rules to a clean run."""
    dept_rows = _dept_rows(date(2024, 6, 15), 1, list(range(1, 10)))  # would fire
    metrics_path, dim_path, _ = _write_detection_inputs(tmp_path, dept_rows)
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--dim-stores-path", str(dim_path),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 0
    df = pd.read_parquet(out / detect_cli.OUTPUT_FILENAME)
    assert (df["rule_id"] == "department_coverage").sum() == 0


def test_cli_exits_nonzero_on_department_metrics_missing_column(tmp_path, rules_path):
    """A department parquet lacking department_id must not be silently used."""
    dept_rows = _dept_rows(date(2024, 6, 15), 1, list(range(1, 11)))
    metrics_path, dim_path, dept_path = _write_detection_inputs(tmp_path, dept_rows)
    pd.read_parquet(dept_path).drop(columns=["department_id"]).to_parquet(
        dept_path, index=False,
    )
    out = tmp_path / "flags"
    exit_code = detect_cli.main([
        "--metrics-path", str(metrics_path),
        "--department-metrics-path", str(dept_path),
        "--dim-stores-path", str(dim_path),
        "--rules-path", str(rules_path),
        "--output-dir", str(out),
    ])
    assert exit_code == 1
