"""End-to-end integration: sim_cli -> detect_cli on real fixture trees.

These tests prove the full chain (ingest the sim engine output, then
evaluate the rules and write the flags parquet) cooperates correctly
across happy and anomalous fixtures, and produces byte-identical
output on repeat runs.
"""

from __future__ import annotations

import hashlib
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src import detect_cli, detect_rules, sim_cli
from src.schemas import ANOMALY_FLAG_COLUMNS, RULE_IDS


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
    assert df["severity_level"].dtype == object or pd.api.types.is_string_dtype(
        df["severity_level"]
    )


def test_flags_parquet_sorted_by_date_store_rule(
    tmp_path, sim_anomalous_root, rules_path
):
    flags_path = _full_pipeline(sim_anomalous_root, tmp_path, rules_path)
    df = pd.read_parquet(flags_path)
    triples = list(zip(df["date"], df["store_id"], df["rule_id"]))
    assert triples == sorted(triples)


# ==============================================================================
# Rolling z-score rule — full orchestrator over a 60-day synthetic frame
# ==============================================================================


def _multi_store_alternating_frame(
    store_specs: list[tuple[int, float, float, float]],
    start: date = date(2024, 1, 1),
) -> pd.DataFrame:
    """Build a 60-day-per-store synthetic store_daily_metrics frame.

    Each ``(store_id, center, half, target_z)`` produces a series that
    alternates ``center ± half`` so the rolling-window stats are
    stable, and whose final day sits at ``target_z`` stddevs from the
    prior 28-day rolling mean (with the same 1e-9 * std nudge the unit
    helper uses to keep the FP round-trip from undershooting).
    """
    rows: list[dict] = []
    for store_id, center, half, target_z in store_specs:
        pattern = [
            center - half if i % 2 == 0 else center + half
            for i in range(60)
        ]
        window = pattern[31:59]
        mean = float(np.mean(window))
        std = float(np.std(window, ddof=1))
        nudge = (1.0 if target_z >= 0 else -1.0) * 1e-9
        pattern[59] = mean + (target_z + nudge) * std
        for i, v in enumerate(pattern):
            rows.append({
                "date": start + timedelta(days=i),
                "store_id": store_id,
                "total_sales": v,
                "transaction_count": 50,
                "avg_basket_size": v / 50.0 if v else 0.0,
                "labor_cost_pct": 0.105,
            })
    df = pd.DataFrame(rows)
    df["store_id"] = df["store_id"].astype(np.int64)
    df["transaction_count"] = df["transaction_count"].astype(np.int64)
    return df


def test_zscore_rule_fires_alongside_other_rules_via_run_all_rules(rules_path):
    """The orchestrator threads `revenue_zscore_28d` through alongside
    the static-band rules: with the canonical YAML config enabled, a
    multi-store 60-day frame whose final day is anomalous fires the
    z-score rule for each store, and the band rules may also fire on
    the same dates without interfering with the z-score output.

    Business-correctness: per-store flag count for the z-score rule
    equals the number of stores with constructed anomalous final
    days; each flag's `expected_low` matches that store's
    independently-derived rolling mean.
    """
    specs = [
        (1, 1000.0, 100.0, 3.5),   # warning
        (2, 5000.0, 500.0, 4.5),   # critical
        (3, 800.0, 80.0, -2.7),    # info, below mean
    ]
    metrics = _multi_store_alternating_frame(specs)
    dim_stores = pd.DataFrame({
        "store_id": pd.Series([1, 2, 3], dtype=np.int64),
        "base_daily_revenue": [1000.0, 5000.0, 800.0],
        "trade_area_profile": ["suburban-family"] * 3,
    })
    config = detect_rules.load_rules_config(rules_path)

    flags = detect_rules.run_all_rules(metrics, dim_stores, config)

    z_flags = flags[flags["rule_id"] == "revenue_zscore_28d"]
    assert len(z_flags) == 3
    assert set(z_flags["store_id"].astype(int)) == {1, 2, 3}
    assert set(z_flags["severity_level"]) == {"warning", "critical", "info"}
    assert set(flags["rule_id"]).issubset(set(RULE_IDS))

    for store_id, center, _, _ in specs:
        store_flag = z_flags[z_flags["store_id"] == store_id].iloc[0]
        assert store_flag["expected_low"] == pytest.approx(center)
        assert store_flag["expected_high"] == pytest.approx(center)
