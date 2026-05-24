"""Contract tests for the department_coverage structural-integrity rule.

These pin the rule against the committed canonical parquets. The
department-grain canonical carries a known set of store-days whose
department row counts deviate from the ten-department baseline — some
with a department missing, some with a department duplicated, injected
upstream by the simulation engine. The rule must fire on exactly those
store-days, and the committed anomaly_flags.parquet must carry those
findings alongside the statistical-band flags.

The contract pattern here differs from the band-rule contract in
test_sim_engine_contract.py: a band rule's contract is "values inside
the band produce no flag," while a structural rule's contract is "the
rule fires on the known structural irregularities and nowhere else." A
failure here means either the rule's behavior drifted or the canonical
anomaly_flags.parquet was regenerated from a different rule set or a
different department-grain canonical.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.detect_rules import load_rules_config, run_all_rules
from src.schemas import ANOMALY_FLAG_COLUMNS

REPO_ROOT = Path(__file__).resolve().parent.parent
CANONICAL = REPO_ROOT / "data" / "processed" / "canonical"
RULES_PATH = REPO_ROOT / "config" / "detection_rules.yaml"

# Department-grain irregularities present in the canonical: store-days
# with 9 rows (a department missing) and with 11 rows (a department
# duplicated). Read directly off department_daily_metrics.parquet by the
# tests below; restated here as the contract figures.
EXPECTED_MISSING = 39
EXPECTED_DUPLICATE = 13
EXPECTED_STRUCTURAL = EXPECTED_MISSING + EXPECTED_DUPLICATE  # 52

# Per-rule flag counts in the committed canonical. The first three are
# the statistical-band rules whose totals pre-date the structural rule;
# `revenue_zscore_28d` is the rolling-baseline rule that fires per its
# learned per-store window. The totals here are the canonical figures
# the file pins — any regeneration that shifts them needs a matching
# update here (and downstream in README + __TESTING_NOTES.md).
EXPECTED_BAND_FLAGS = {
    "revenue_band": 297,
    "transactions_band": 345,
    "yoy_comp": 189,
    "revenue_zscore_28d": 11,
}


def _irregular_store_days() -> set:
    """The (date, store_id) pairs whose department row count is not ten,
    read straight off the committed department-grain canonical."""
    department = pd.read_parquet(CANONICAL / "department_daily_metrics.parquet")
    row_counts = department.groupby(["date", "store_id"]).size()
    return set(row_counts[row_counts != 10].index)


def _run_detection() -> pd.DataFrame:
    """Run every rule against the committed canonical metric parquets."""
    metrics = pd.read_parquet(CANONICAL / "store_daily_metrics.parquet")
    department = pd.read_parquet(CANONICAL / "department_daily_metrics.parquet")
    dim_stores = pd.read_parquet(CANONICAL / "dim_stores.parquet")
    config = load_rules_config(RULES_PATH)
    return run_all_rules(
        metrics, dim_stores, config, department_metrics_df=department,
    )


def test_rule_fires_on_exactly_the_irregular_canonical_store_days():
    """The rule flags every store-day whose department row count is not
    ten, and no well-formed store-day."""
    irregular = _irregular_store_days()
    structural = _run_detection().query("rule_id == 'department_coverage'")
    flagged = set(zip(structural["date"], structural["store_id"]))

    assert flagged == irregular
    assert len(structural) == EXPECTED_STRUCTURAL


def test_rule_distinguishes_missing_from_duplicated_departments():
    """actual_value carries the observed row count, so a missing
    department (9) and a duplicated one (11) stay distinguishable."""
    structural = _run_detection().query("rule_id == 'department_coverage'")

    assert (structural["actual_value"] == 9.0).sum() == EXPECTED_MISSING
    assert (structural["actual_value"] == 11.0).sum() == EXPECTED_DUPLICATE
    assert set(structural["severity_level"]) == {"warning"}
    assert (structural["expected_low"] == 10.0).all()
    assert (structural["expected_high"] == 10.0).all()
    assert (structural["distance_from_band"] == 1.0).all()


def test_well_formed_store_days_are_not_flagged():
    """The store-days with exactly ten departments are left clean, and
    they are the large majority of the canonical."""
    department = pd.read_parquet(CANONICAL / "department_daily_metrics.parquet")
    row_counts = department.groupby(["date", "store_id"]).size()
    well_formed = set(row_counts[row_counts == 10].index)

    structural = _run_detection().query("rule_id == 'department_coverage'")
    flagged = set(zip(structural["date"], structural["store_id"]))

    assert flagged.isdisjoint(well_formed)
    assert len(well_formed) == len(row_counts) - EXPECTED_STRUCTURAL


def test_committed_anomaly_flags_carries_structural_findings():
    """The committed canonical anomaly_flags.parquet includes the
    structural flags alongside the unchanged statistical-band flags."""
    flags = pd.read_parquet(CANONICAL / "anomaly_flags.parquet")
    counts = flags["rule_id"].value_counts().to_dict()

    assert tuple(flags.columns) == ANOMALY_FLAG_COLUMNS
    assert counts["department_coverage"] == EXPECTED_STRUCTURAL
    for rule_id, expected in EXPECTED_BAND_FLAGS.items():
        assert counts[rule_id] == expected
    assert len(flags) == EXPECTED_STRUCTURAL + sum(EXPECTED_BAND_FLAGS.values())
