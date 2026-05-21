"""Upstream contract tests: real sim engine output -> ETL canonical values.

Unlike the hand-authored fixtures under ``tests/fixtures/sim_engine/`` —
which carry deliberately clean round numbers so the detection-rule tests
can assert predictable outcomes — the fixture under
``tests/fixtures/sim_engine_contract/`` is a verbatim capture of the
simulation engine's realism-applied output for 2024-07-01 (8 stores, 10
departments each). See that directory's README for provenance.

These tests pin that captured input and assert the specific values the ETL
transform and detection layers produce from it. Every expected value is
derived independently from the source CSV rows, not from a prior run of
the ETL. They fail if the sim engine's output schema drifts or if an ETL
transform silently changes a computed value.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src import detect_rules, sim_ingest, sim_transform

REPO_ROOT = Path(__file__).resolve().parent.parent
CONTRACT_ROOT = REPO_ROOT / "tests" / "fixtures" / "sim_engine_contract" / "output"
RULES_PATH = REPO_ROOT / "config" / "detection_rules.yaml"
CONTRACT_DATE = date(2024, 7, 1)


# ==============================================================================
# Store-day grain
# ==============================================================================


def test_store_day_transform_produces_expected_values():
    """The store-day transform of the captured 2024-07-01 output yields the
    exact total_sales / transaction_count / labor_cost_pct from the source
    CSV, with avg_basket_size equal to total_sales / transaction_count.

    Source rows (store_summary.csv):
        2024-07-01,1,86429.35,86429.35,2337,9923.32,0.1148
        2024-07-01,2,101233.48,101233.48,2625,11268.61,0.1113
        2024-07-01,8,49195.84,49195.84,1532,6348.91,0.1291
    """
    records = list(sim_ingest.load_store_summaries(CONTRACT_ROOT))
    dim_stores = sim_ingest.load_dim_stores(CONTRACT_ROOT)
    metrics = sim_transform.build_store_daily_metrics(records, dim_stores)

    assert len(metrics) == 8
    assert set(metrics["store_id"]) == set(range(1, 9))
    assert set(metrics["date"]) == {CONTRACT_DATE}

    by_store = metrics.set_index("store_id")

    assert by_store.loc[1, "total_sales"] == pytest.approx(86429.35)
    assert by_store.loc[1, "transaction_count"] == 2337
    assert by_store.loc[1, "labor_cost_pct"] == pytest.approx(0.1148)
    assert by_store.loc[1, "avg_basket_size"] == pytest.approx(86429.35 / 2337)

    assert by_store.loc[2, "total_sales"] == pytest.approx(101233.48)
    assert by_store.loc[2, "transaction_count"] == 2625
    assert by_store.loc[2, "avg_basket_size"] == pytest.approx(101233.48 / 2625)

    assert by_store.loc[8, "total_sales"] == pytest.approx(49195.84)
    assert by_store.loc[8, "transaction_count"] == 1532
    assert by_store.loc[8, "labor_cost_pct"] == pytest.approx(0.1291)


# ==============================================================================
# Department grain + cross-grain reconciliation
# ==============================================================================


def test_department_transform_and_cross_grain_reconciliation():
    """The department transform yields the exact per-department values from
    the source CSV, and the sum of a store's department net_sales equals
    that store's net_sales_total in store_summary.csv — the two grains must
    reconcile.

    Source rows (department_sales.csv):
        2024-07-01,1,1,12846.11,0.0,12846.11,...,0.48,336,1082,...
        2024-07-01,7,7,13803.2,0.0,13803.2,...,0.2659,419,2004,...
    """
    dept_records = list(sim_ingest.load_department_sales(CONTRACT_ROOT))
    store_records = list(sim_ingest.load_store_summaries(CONTRACT_ROOT))
    dim_stores = sim_ingest.load_dim_stores(CONTRACT_ROOT)

    dept = sim_transform.build_department_daily_metrics(dept_records, dim_stores)

    assert len(dept) == 80  # 8 stores x 10 departments
    assert set(dept["store_id"]) == set(range(1, 9))
    assert set(dept["department_id"]) == set(range(1, 11))

    s1d1 = dept[(dept["store_id"] == 1) & (dept["department_id"] == 1)].iloc[0]
    assert s1d1["net_sales"] == pytest.approx(12846.11)
    assert s1d1["transactions"] == 336
    assert s1d1["units_sold"] == 1082
    assert s1d1["gross_margin_pct"] == pytest.approx(0.48)

    s7d7 = dept[(dept["store_id"] == 7) & (dept["department_id"] == 7)].iloc[0]
    assert s7d7["net_sales"] == pytest.approx(13803.20)
    assert s7d7["transactions"] == 419
    assert s7d7["units_sold"] == 2004

    # Cross-grain invariant: per store, the department net_sales must sum to
    # the store_summary net_sales_total. The sim engine derives the store
    # total from its department detail, so the two ETL grains must agree.
    store_totals = {r.store_id: r.net_sales_total for r in store_records}
    dept_sums = dept.groupby("store_id")["net_sales"].sum()
    for store_id, total in store_totals.items():
        assert dept_sums[store_id] == pytest.approx(total, abs=0.01)


# ==============================================================================
# Detection
# ==============================================================================


def test_detection_produces_no_flags_on_normal_sim_day():
    """2024-07-01 is a normal store-day in the captured output — its
    anomaly_log is empty. Every store's revenue, labor, basket and
    transaction values fall inside the static detection bands, so the rules
    engine must emit zero flags: the ETL must not false-positive on
    ordinary sim engine output.

    The perturbation at the end confirms the zero-flag result is real and
    not a vacuous pass from a broken rules engine.
    """
    records = list(sim_ingest.load_store_summaries(CONTRACT_ROOT))
    dim_stores = sim_ingest.load_dim_stores(CONTRACT_ROOT)
    metrics = sim_transform.build_store_daily_metrics(records, dim_stores)
    rules_config = detect_rules.load_rules_config(RULES_PATH)

    flags = detect_rules.run_all_rules(metrics, dim_stores, rules_config)

    assert len(metrics) == 8  # detection evaluated all 8 stores
    assert len(flags) == 0

    # Sanity: detection is live. Pushing store 1 far below its revenue band
    # (base_daily_revenue 95000, band ±25%) must make revenue_band fire.
    perturbed = metrics.copy()
    perturbed.loc[perturbed["store_id"] == 1, "total_sales"] = 10000.0
    perturbed_flags = detect_rules.run_all_rules(
        perturbed, dim_stores, rules_config
    )
    fired = set(zip(perturbed_flags["store_id"], perturbed_flags["rule_id"]))
    assert (1, "revenue_band") in fired
