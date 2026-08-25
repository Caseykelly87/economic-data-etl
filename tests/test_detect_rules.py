"""Tests for src.detect_rules — pure-pandas exception detection rules.

Every rule is exercised in isolation with synthetic in-memory frames so
band edges, severity buckets, and skip behaviors are pinned without
involvement from the YAML loader or the parquet IO layer.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src import detect_rules
from src.exceptions import DetectionConfigError, DetectionInputError
from src.schemas import ANOMALY_FLAG_COLUMNS, RULE_IDS, SEVERITY_LEVELS

# ----- helpers ----------------------------------------------------------------


def _metric_row(
    d: date,
    store_id: int,
    sales: float,
    txns: int,
    labor_pct: float,
) -> dict:
    basket = sales / txns if txns > 0 else float("nan")
    return {
        "date": d,
        "store_id": store_id,
        "total_sales": sales,
        "transaction_count": txns,
        "avg_basket_size": basket,
        "labor_cost_pct": labor_pct,
    }


def _metrics_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["store_id"] = df["store_id"].astype(np.int64)
    df["transaction_count"] = df["transaction_count"].astype(np.int64)
    return df


def _single_store_dim(store_id: int, base: float, profile: str) -> pd.DataFrame:
    return pd.DataFrame({
        "store_id": [store_id],
        "base_daily_revenue": [base],
        "trade_area_profile": [profile],
    })


def _dept_row(d: date, store_id: int, dept_id: int) -> dict:
    """One department_daily_metrics row. Values past the first three
    columns are filler — the structural rule reads only date, store_id,
    and department_id."""
    return {
        "date": d,
        "store_id": store_id,
        "department_id": dept_id,
        "net_sales": 1000.0,
        "transactions": 50,
        "units_sold": 120,
        "gross_margin_pct": 0.30,
    }


def _dept_df(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["store_id"] = df["store_id"].astype(np.int64)
    df["department_id"] = df["department_id"].astype(np.int64)
    return df


def _ten_departments(d: date, store_id: int) -> list[dict]:
    """A well-formed store-day: ten distinct department rows."""
    return [_dept_row(d, store_id, dept_id) for dept_id in range(1, 11)]


# ==============================================================================
# load_rules_config
# ==============================================================================


def test_load_rules_config_returns_dict(tmp_path):
    real = Path(__file__).resolve().parent.parent / "config" / "detection_rules.yaml"
    cfg = detect_rules.load_rules_config(real)
    assert isinstance(cfg, dict)
    assert "rules" in cfg and "severity" in cfg


def test_load_rules_config_missing_section_raises(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("severity:\n  info_max: 1.0\n", encoding="utf-8")
    with pytest.raises(DetectionConfigError):
        detect_rules.load_rules_config(bad)


def test_load_rules_config_unknown_profile_raises(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "rules:\n"
        "  revenue_band: { enabled: true, band_pct: 0.25 }\n"
        "  labor_pct_band:\n"
        "    enabled: true\n"
        "    bands_by_profile:\n"
        "      mystery-profile: { center: 0.10, half_width_pp: 0.05 }\n"
        "  avg_ticket_band: { enabled: false }\n"
        "  transactions_band: { enabled: false }\n"
        "  yoy_comp: { enabled: false }\n"
        "  revenue_zscore_28d: { enabled: false }\n"
        "  department_coverage: { enabled: false }\n"
        "  gross_margin_band: { enabled: false }\n"
        "  department_reconciliation: { enabled: false }\n"
        "severity: { info_max: 1.0, warning_max: 2.0 }\n",
        encoding="utf-8",
    )
    with pytest.raises(DetectionConfigError) as excinfo:
        detect_rules.load_rules_config(bad)
    assert "mystery-profile" in str(excinfo.value)


# ==============================================================================
# run_all_rules — orchestrator-level invariants
# ==============================================================================


def test_orchestrator_columns_match_anomaly_flag_schema(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config
    )
    assert tuple(result.columns) == ANOMALY_FLAG_COLUMNS


def test_orchestrator_empty_input_returns_empty_frame(
    sample_dim_stores_df, detection_rules_config
):
    empty = pd.DataFrame(
        columns=["date", "store_id", "total_sales", "transaction_count",
                 "avg_basket_size", "labor_cost_pct"]
    )
    result = detect_rules.run_all_rules(empty, sample_dim_stores_df, detection_rules_config)
    assert tuple(result.columns) == ANOMALY_FLAG_COLUMNS
    assert len(result) == 0


def test_orchestrator_all_in_band_returns_empty_frame(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """sample_metrics_df is constructed inside-band on every dimension."""
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config
    )
    assert len(result) == 0


def test_orchestrator_sorted_by_date_store_rule(
    sample_dim_stores_df, detection_rules_config
):
    rows = [
        _metric_row(date(2024, 6, 15), 1, 10000.0, 263, 0.30),  # multiple breaches
        _metric_row(date(2024, 6, 16), 1, 10000.0, 263, 0.30),
    ]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert len(result) > 0
    triples = list(zip(result["date"], result["store_id"], result["rule_id"]))
    assert triples == sorted(triples)


def test_orchestrator_idempotent(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    df = sample_metrics_df.copy()
    df.loc[0, "total_sales"] = 5000.0
    df.loc[0, "labor_cost_pct"] = 0.05
    a = detect_rules.run_all_rules(df, sample_dim_stores_df, detection_rules_config)
    b = detect_rules.run_all_rules(df, sample_dim_stores_df, detection_rules_config)
    assert a.equals(b)


def test_orchestrator_orphan_store_id_raises(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 999, 50000.0, 1500, 0.10)]
    with pytest.raises(DetectionInputError) as excinfo:
        detect_rules.run_all_rules(
            _metrics_df(rows), sample_dim_stores_df, detection_rules_config
        )
    assert "999" in str(excinfo.value)


def test_orchestrator_disabled_rule_produces_no_flags(
    sample_dim_stores_df, detection_rules_config
):
    """A rule with enabled: false must never emit a flag."""
    cfg = detection_rules_config.copy()
    cfg["rules"] = {k: v.copy() for k, v in cfg["rules"].items()}
    cfg["rules"]["revenue_band"]["enabled"] = False
    rows = [_metric_row(date(2024, 6, 15), 1, 5000.0, 1900, 0.105)]  # would fire revenue_band
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, cfg
    )
    assert (result["rule_id"] == "revenue_band").sum() == 0


# ==============================================================================
# revenue_band
# ==============================================================================


def test_revenue_band_in_band_no_flag(sample_dim_stores_df, detection_rules_config):
    rows = [_metric_row(date(2024, 6, 15), 1, 95000.0, 2500, 0.105)]  # exactly at expected
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "revenue_band").sum() == 0


def test_revenue_band_just_outside_lower_edge_info_severity(
    sample_dim_stores_df, detection_rules_config
):
    # base 95000, band ±0.60 → lower edge 38000, half_width 57000.
    # 37000 is 1000 past the edge → score 0.018 → info.
    rows = [_metric_row(date(2024, 6, 15), 1, 37000.0, 974, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "revenue_band"]
    assert len(flags) == 1
    assert flags["severity_level"].iloc[0] == "info"


def test_revenue_band_far_outside_critical_severity(
    sample_dim_stores_df, detection_rules_config
):
    # base 95000, band ±0.60 → upper edge 152000, half_width 57000. score > 2
    # needs distance > 114000, i.e. total_sales > 266000. Use 270000.
    # (With the wide band the lower edge sits at 38000, so even total_sales
    # of 0 lands at score 0.67 — only a high-side excursion reaches critical.)
    rows = [_metric_row(date(2024, 6, 15), 1, 270000.0, 7105, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "revenue_band"]
    assert len(flags) == 1
    assert flags["severity_level"].iloc[0] == "critical"
    assert flags["distance_from_band"].iloc[0] > 0


def test_revenue_band_actual_above_upper_edge_fires(
    sample_dim_stores_df, detection_rules_config
):
    # base 95000, band ±0.60 → upper edge 152000. 160000 is above → fires.
    rows = [_metric_row(date(2024, 6, 15), 1, 160000.0, 4210, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "revenue_band"]
    assert len(flags) == 1
    assert flags["actual_value"].iloc[0] == pytest.approx(160000.0)
    assert flags["distance_from_band"].iloc[0] == pytest.approx(8000.0)


# ==============================================================================
# labor_pct_band
# ==============================================================================


def test_labor_pct_band_in_band_no_flag(sample_dim_stores_df, detection_rules_config):
    rows = [_metric_row(date(2024, 6, 15), 1, 87400.0, 2300, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "labor_pct_band").sum() == 0


def test_labor_pct_band_above_upper_edge_fires(
    sample_dim_stores_df, detection_rules_config
):
    # suburban-family center 0.105, half_width 0.05 → upper 0.155.
    rows = [_metric_row(date(2024, 6, 15), 1, 87400.0, 2300, 0.20)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "labor_pct_band"]
    assert len(flags) == 1
    assert flags["actual_value"].iloc[0] == pytest.approx(0.20)


def test_labor_pct_band_skipped_when_total_sales_zero(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 1, 0.0, 0, 0.50)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "labor_pct_band").sum() == 0


def test_labor_pct_band_skipped_when_pct_nan(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 1, 87400.0, 2300, float("nan"))]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "labor_pct_band").sum() == 0


# ==============================================================================
# avg_ticket_band
# ==============================================================================


def test_avg_ticket_band_in_band_no_flag(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 4, 56000.0, 2000, 0.115)]  # basket 28.0
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "avg_ticket_band").sum() == 0


def test_avg_ticket_band_above_upper_edge_fires(
    sample_dim_stores_df, detection_rules_config
):
    # urban-dense center 28, band_pct 0.20 → upper 33.6.
    # 56000 / 1000 = 56 → way above.
    rows = [_metric_row(date(2024, 6, 15), 4, 56000.0, 1000, 0.115)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "avg_ticket_band"]
    assert len(flags) == 1
    assert flags["severity_level"].iloc[0] == "critical"


def test_avg_ticket_band_skipped_when_total_sales_zero(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 4, 0.0, 0, 0.115)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "avg_ticket_band").sum() == 0


# ==============================================================================
# transactions_band
# ==============================================================================


def test_transactions_band_in_band_no_flag(
    sample_dim_stores_df, detection_rules_config
):
    # store 7: base 55000 / avg_ticket 32 = 1718.75; band ±0.45 → [945, 2490].
    rows = [_metric_row(date(2024, 6, 15), 7, 50600.0, 1581, 0.120)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "transactions_band").sum() == 0


def test_transactions_band_below_lower_edge_fires(
    sample_dim_stores_df, detection_rules_config
):
    # store 7 expected 1718.75, band ±0.45 → [945, 2490]. 600 way below.
    rows = [_metric_row(date(2024, 6, 15), 7, 50600.0, 600, 0.120)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "transactions_band"]
    assert len(flags) == 1
    assert flags["actual_value"].iloc[0] == pytest.approx(600.0)


def test_transactions_band_skipped_when_total_sales_zero(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 7, 0.0, 0, 0.120)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "transactions_band").sum() == 0


# ==============================================================================
# yoy_comp
# ==============================================================================


def test_yoy_comp_silently_skipped_without_t365_row(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 1, 87400.0, 2300, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "yoy_comp").sum() == 0


def test_yoy_comp_in_range_no_flag(
    sample_dim_stores_df, detection_rules_config
):
    """Same revenue both years → ratio 1.0, in [0.85, 1.25], no flag.

    The pair 2024-06-15 / 2025-06-15 is exactly 365 days apart (no leap
    day inside that span), so the lookup hits.
    """
    rows = [
        _metric_row(date(2024, 6, 15), 1, 87400.0, 2300, 0.105),
        _metric_row(date(2024, 6, 14), 1, 87400.0, 2300, 0.105),  # near miss
        _metric_row(date(2025, 6, 15), 1, 87400.0, 2300, 0.105),
    ]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "yoy_comp").sum() == 0


def test_yoy_comp_below_threshold_fires(
    sample_dim_stores_df, detection_rules_config
):
    """Prior 100000, current 50000 → ratio 0.5 < 0.55."""
    rows = [
        _metric_row(date(2024, 6, 15), 1, 100000.0, 2632, 0.105),
        _metric_row(date(2025, 6, 14), 1, 87400.0, 2300, 0.105),
        _metric_row(date(2025, 6, 15), 1, 50000.0, 1316, 0.105),
    ]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "yoy_comp"]
    assert len(flags) == 1
    assert flags["actual_value"].iloc[0] == pytest.approx(0.5)


def test_yoy_comp_above_threshold_fires(
    sample_dim_stores_df, detection_rules_config
):
    """Prior 50000, current 100000 → ratio 2.0 > 1.40."""
    rows = [
        _metric_row(date(2024, 6, 15), 1, 50000.0, 1316, 0.105),
        _metric_row(date(2025, 6, 15), 1, 100000.0, 2632, 0.105),
    ]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "yoy_comp"]
    assert len(flags) == 1


# ==============================================================================
# department_coverage
# ==============================================================================


def test_department_coverage_ten_distinct_departments_no_flag(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A store-day with ten distinct departments is well-formed."""
    dept = _dept_df(_ten_departments(date(2024, 6, 15), 1))
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=dept,
    )
    assert (result["rule_id"] == "department_coverage").sum() == 0


def test_department_coverage_missing_department_fires_warning(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Nine rows — department 10 absent — fires one warning flag."""
    rows = [_dept_row(date(2024, 6, 15), 1, dept_id) for dept_id in range(1, 10)]
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "department_coverage"]
    assert len(flags) == 1
    flag = flags.iloc[0]
    assert flag["store_id"] == 1
    assert flag["actual_value"] == 9.0
    assert flag["expected_low"] == 10.0
    assert flag["expected_high"] == 10.0
    assert flag["distance_from_band"] == 1.0
    assert flag["severity_level"] == "warning"


def test_department_coverage_duplicate_department_fires_warning(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Eleven rows — department 3 repeated — fires one warning flag."""
    rows = _ten_departments(date(2024, 6, 15), 1)
    rows.append(_dept_row(date(2024, 6, 15), 1, 3))
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "department_coverage"]
    assert len(flags) == 1
    flag = flags.iloc[0]
    assert flag["actual_value"] == 11.0
    assert flag["distance_from_band"] == 1.0
    assert flag["severity_level"] == "warning"


def test_department_coverage_duplicate_with_expected_count_still_fires(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Ten rows, but department 7 is missing and department 3 is doubled:
    the count matches expected while the shape is still wrong, so the
    duplicate condition alone fires the flag."""
    rows = [
        _dept_row(date(2024, 6, 15), 1, d)
        for d in (1, 2, 3, 3, 4, 5, 6, 8, 9, 10)
    ]
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "department_coverage"]
    assert len(flags) == 1
    assert flags.iloc[0]["actual_value"] == 10.0
    assert flags.iloc[0]["distance_from_band"] == 0.0


def test_department_coverage_detect_duplicates_disabled_ignores_duplicate(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """With detect_duplicates off, a duplicate at the expected count does
    not fire."""
    cfg = detection_rules_config.copy()
    cfg["rules"] = {k: v.copy() for k, v in cfg["rules"].items()}
    cfg["rules"]["department_coverage"]["detect_duplicates"] = False
    rows = [
        _dept_row(date(2024, 6, 15), 1, d)
        for d in (1, 2, 3, 3, 4, 5, 6, 8, 9, 10)
    ]
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, cfg,
        department_metrics_df=_dept_df(rows),
    )
    assert (result["rule_id"] == "department_coverage").sum() == 0


def test_department_coverage_skipped_without_department_frame(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """With no department frame supplied the structural rule contributes
    nothing to the flags output."""
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
    )
    assert (result["rule_id"] == "department_coverage").sum() == 0


def test_department_coverage_disabled_produces_no_flags(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A rule with enabled: false must never emit a flag even when the
    department frame contains a clear irregularity."""
    cfg = detection_rules_config.copy()
    cfg["rules"] = {k: v.copy() for k, v in cfg["rules"].items()}
    cfg["rules"]["department_coverage"]["enabled"] = False
    rows = [_dept_row(date(2024, 6, 15), 1, dept_id) for dept_id in range(1, 10)]
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, cfg,
        department_metrics_df=_dept_df(rows),
    )
    assert (result["rule_id"] == "department_coverage").sum() == 0


def test_department_coverage_flags_only_offending_store_days(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Across four store-days only the two irregular ones fire."""
    d1, d2 = date(2024, 6, 15), date(2024, 6, 16)
    rows: list[dict] = []
    rows += _ten_departments(d1, 1)                              # clean
    rows += [_dept_row(d1, 2, dept) for dept in range(1, 10)]    # 9 — missing
    rows += _ten_departments(d2, 1)
    rows.append(_dept_row(d2, 1, 5))                             # 11 — duplicate
    rows += _ten_departments(d2, 2)                              # clean
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "department_coverage"]
    fired = set(zip(flags["date"], flags["store_id"].astype(int)))
    assert fired == {(d1, 2), (d2, 1)}


def test_department_coverage_output_matches_anomaly_flag_schema(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Structural flags carry the same schema and vocabulary as band flags."""
    rows = [_dept_row(date(2024, 6, 15), 1, dept_id) for dept_id in range(1, 10)]
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    assert tuple(result.columns) == ANOMALY_FLAG_COLUMNS
    flags = result[result["rule_id"] == "department_coverage"]
    assert set(flags["rule_id"]).issubset(set(RULE_IDS))
    assert set(flags["severity_level"]).issubset(set(SEVERITY_LEVELS))
    assert (flags["severity_score"] == 1.0).all()


# ==============================================================================
# gross_margin_band
# ==============================================================================


def test_gross_margin_band_high_outlier_fires_warning(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A department at a 0.95 gross margin — the injected high-margin shape
    — fires one warning flag for its store-day.

    Business-correctness: the band is center 0.385 ± 0.235, so the upper
    edge is 0.62. distance = 0.95 - 0.62 = 0.33 and severity_score =
    0.33 / 0.235 = 1.404, which lands in the warning bucket (1 < score ≤ 2).
    The fired flag and its hand-computed score are both asserted.
    """
    high = 0.385 + 0.235
    rows = _ten_departments(date(2024, 6, 15), 1)
    rows[2]["gross_margin_pct"] = 0.95
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "gross_margin_band"]
    assert len(flags) == 1
    flag = flags.iloc[0]
    assert flag["store_id"] == 1
    assert flag["actual_value"] == pytest.approx(0.95)
    assert flag["expected_high"] == pytest.approx(high)
    assert flag["distance_from_band"] == pytest.approx(0.95 - high)
    assert flag["severity_score"] == pytest.approx((0.95 - high) / 0.235)
    assert flag["severity_level"] == "warning"


def test_gross_margin_band_negative_margin_fires(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A department at a negative gross margin — the injected
    COGS-exceeds-sales shape — fires below the band's lower edge.

    Business-correctness: lower edge 0.385 - 0.235 = 0.15. A -0.20 margin
    sits 0.35 below it; the flag's actual_value and the lower-edge
    distance are hand-derived from the configured band.
    """
    low = 0.385 - 0.235
    rows = _ten_departments(date(2024, 6, 15), 1)
    rows[4]["gross_margin_pct"] = -0.20
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "gross_margin_band"]
    assert len(flags) == 1
    flag = flags.iloc[0]
    assert flag["actual_value"] == pytest.approx(-0.20)
    assert flag["expected_low"] == pytest.approx(low)
    assert flag["distance_from_band"] == pytest.approx(low - (-0.20))


def test_gross_margin_band_inside_band_no_flag(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Every department inside [0.15, 0.62], including values near both
    edges, leaves the store-day clean.

    Business-correctness: 0.20 and 0.55 are inside the band, so no margin
    flag is emitted — confirming the rule does not fire on natural margin
    variance.
    """
    rows = _ten_departments(date(2024, 6, 15), 1)
    rows[0]["gross_margin_pct"] = 0.20
    rows[1]["gross_margin_pct"] = 0.55
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    assert (result["rule_id"] == "gross_margin_band").sum() == 0


def test_gross_margin_band_one_flag_per_store_day_picks_worst(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """When two departments are out of band, the store-day yields a single
    flag carrying the one furthest past an edge.

    Business-correctness: 0.95 sits 0.33 above the upper edge while -0.20
    sits 0.35 below the lower edge; the larger distance wins, so the flag's
    actual_value is -0.20 and exactly one flag is emitted for the store-day.
    """
    rows = _ten_departments(date(2024, 6, 15), 1)
    rows[2]["gross_margin_pct"] = 0.95
    rows[7]["gross_margin_pct"] = -0.20
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "gross_margin_band"]
    assert len(flags) == 1
    assert flags.iloc[0]["actual_value"] == pytest.approx(-0.20)


def test_gross_margin_band_skipped_without_department_frame(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """With no department frame supplied the rule contributes nothing —
    margin lives only at department grain.

    Ceremony: confirms the skip path raises nothing and emits no flag, the
    same skip contract department_coverage carries.
    """
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
    )
    assert (result["rule_id"] == "gross_margin_band").sum() == 0


# ==============================================================================
# department_reconciliation
# ==============================================================================


def _balanced_departments(d: date, store_id: int, store_total: float) -> list[dict]:
    """Ten department rows whose net_sales sum exactly to ``store_total``."""
    rows = _ten_departments(d, store_id)
    share = store_total / len(rows)
    for r in rows:
        r["net_sales"] = share
    return rows


# sample_metrics_df sets every store-day to base_daily_revenue * 0.92;
# store 1's base is 95000, so its 2024-06-15 total_sales is 87400.0.
_STORE_1_TOTAL = 95000.0 * 0.92


def test_department_reconciliation_balanced_no_flag(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """Department net_sales that sum to the store total leave the store-day
    clean.

    Business-correctness: ten departments of 8740.0 sum to 87400.0, which
    equals store 1's store-day total_sales, so the residual is zero and no
    reconciliation flag is emitted.
    """
    rows = _balanced_departments(date(2024, 6, 15), 1, _STORE_1_TOTAL)
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    assert (result["rule_id"] == "department_reconciliation").sum() == 0


def test_department_reconciliation_mismatch_beyond_tolerance_fires(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A $100 break between the department sum and the store total fires one
    flag with the residual carried as the distance.

    Business-correctness: adding 100.0 to one department makes the
    department sum 87500.0 against a store total of 87400.0 — a residual of
    100.0, well past the $1.00 tolerance. actual_value carries the
    department sum, expected_low/high the store total, and severity is the
    structural fixed warning.
    """
    rows = _balanced_departments(date(2024, 6, 15), 1, _STORE_1_TOTAL)
    rows[0]["net_sales"] += 100.0
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    flags = result[result["rule_id"] == "department_reconciliation"]
    assert len(flags) == 1
    flag = flags.iloc[0]
    assert flag["store_id"] == 1
    assert flag["actual_value"] == pytest.approx(_STORE_1_TOTAL + 100.0)
    assert flag["expected_low"] == pytest.approx(_STORE_1_TOTAL)
    assert flag["expected_high"] == pytest.approx(_STORE_1_TOTAL)
    assert flag["distance_from_band"] == pytest.approx(100.0)
    assert flag["severity_score"] == pytest.approx(1.0)
    assert flag["severity_level"] == "warning"


def test_department_reconciliation_within_tolerance_no_flag(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """A sub-dollar residual is absorbed by the tolerance.

    Business-correctness: a 0.50 break (below the $1.00 tolerance) is the
    kind of rounding the rule must ignore, so no flag is emitted.
    """
    rows = _balanced_departments(date(2024, 6, 15), 1, _STORE_1_TOTAL)
    rows[0]["net_sales"] += 0.50
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
        department_metrics_df=_dept_df(rows),
    )
    assert (result["rule_id"] == "department_reconciliation").sum() == 0


def test_department_reconciliation_skipped_without_department_frame(
    sample_metrics_df, sample_dim_stores_df, detection_rules_config
):
    """With no department frame the cross-grain rule contributes nothing.

    Ceremony: confirms the skip path raises nothing and emits no flag.
    """
    result = detect_rules.run_all_rules(
        sample_metrics_df, sample_dim_stores_df, detection_rules_config,
    )
    assert (result["rule_id"] == "department_reconciliation").sum() == 0


# ==============================================================================
# severity scoring math
# ==============================================================================


def test_severity_score_equals_distance_over_band_half_width(
    sample_dim_stores_df, detection_rules_config
):
    # Construct revenue_band breach with known math.
    # base 95000, band ±0.60 → [38000, 152000], half_width 57000.
    # actual 30000 → distance 8000 → score 8000/57000 = 0.1404
    rows = [_metric_row(date(2024, 6, 15), 1, 30000.0, 789, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flag = result[result["rule_id"] == "revenue_band"].iloc[0]
    assert flag["distance_from_band"] == pytest.approx(8000.0)
    assert flag["severity_score"] == pytest.approx(8000.0 / 57000.0)
    assert flag["severity_level"] == "info"


def test_distance_from_band_always_nonneg(
    sample_dim_stores_df, detection_rules_config
):
    rows = [
        _metric_row(date(2024, 6, 15), 1, 10000.0, 263, 0.50),  # below revenue, above labor
        _metric_row(date(2024, 6, 15), 4, 200000.0, 7142, 0.115),  # above revenue
    ]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["distance_from_band"] >= 0).all()


def test_severity_levels_in_canonical_set(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 1, 10000.0, 263, 0.50)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert set(result["severity_level"]).issubset(set(SEVERITY_LEVELS))


def test_rule_ids_in_canonical_set(
    sample_dim_stores_df, detection_rules_config
):
    rows = [_metric_row(date(2024, 6, 15), 1, 10000.0, 263, 0.50)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert set(result["rule_id"]).issubset(set(RULE_IDS))


# ==============================================================================
# revenue_zscore_28d
# ==============================================================================


def _zscore_dim_stores(store_ids: list[int], base: float = 1000.0) -> pd.DataFrame:
    """dim_stores covering ``store_ids`` with a uniform ``base_daily_revenue``.

    The base is chosen by the caller so the static `revenue_band` rule
    does not fire on the synthetic z-score data; the z-score tests
    further disable the other rules through ``_only_zscore_config`` so
    only the rule under test contributes flags.
    """
    return pd.DataFrame({
        "store_id": pd.Series(store_ids, dtype=np.int64),
        "base_daily_revenue": [base] * len(store_ids),
        "trade_area_profile": ["suburban-family"] * len(store_ids),
    })


def _only_zscore_config(detection_rules_config: dict) -> dict:
    """Deep-copy the config with every rule but `revenue_zscore_28d` disabled."""
    cfg = {**detection_rules_config}
    cfg["rules"] = {k: dict(v) for k, v in cfg["rules"].items()}
    for rule_id, rule_cfg in cfg["rules"].items():
        rule_cfg["enabled"] = rule_id == "revenue_zscore_28d"
    return cfg


def _zscore_rows(
    store_id: int, sales: list[float], start: date = date(2024, 1, 1),
) -> list[dict]:
    """One synthetic store_daily_metrics row per consecutive day starting
    at ``start``. Non-sales fields are filler; the z-score rule reads
    only `date`, `store_id`, and `total_sales`."""
    return [
        {
            "date": start + timedelta(days=i),
            "store_id": store_id,
            "total_sales": v,
            "transaction_count": 50,
            "avg_basket_size": v / 50.0 if v else 0.0,
            "labor_cost_pct": 0.105,
        }
        for i, v in enumerate(sales)
    ]


def _alternating_with_target(
    center: float, half: float, target_z: float,
) -> tuple[list[float], float, float]:
    """Build a 60-day alternating-value series whose final day sits at
    ``target_z`` stddevs from the prior 28-day rolling mean.

    The whole series alternates ``center - half`` / ``center + half`` so
    every interior rolling window is the same (mean=center, std stable),
    and the resulting per-row |z| for non-final days stays under the
    rule's 2.5 trigger threshold — only the final day fires.
    Returns the sales list along with the expected mean and stddev for
    the final day's window so the test can hand-check the flag's
    `expected_low`, `expected_high`, and `severity_score`.

    The target is nudged by ``1e-9 * sign(target_z) * stddev`` so the
    computed z lands at or just past ``target_z`` after the
    subtract-then-divide round-trip (`mean + 3*std` then `(x - mean) /
    std` would otherwise yield 2.9999999... at the exact 3.0 boundary
    and bucket-flip into ``info``).
    """
    pattern = [center - half if i % 2 == 0 else center + half for i in range(60)]
    window = pattern[31:59]
    expected_mean = float(np.mean(window))
    expected_std = float(np.std(window, ddof=1))
    nudge = (1.0 if target_z >= 0 else -1.0) * 1e-9
    pattern[59] = expected_mean + (target_z + nudge) * expected_std
    return pattern, expected_mean, expected_std


class TestRevenueZscore28d:
    """Business-correctness tests for the rolling z-score rule.

    Each test computes its expected value(s) from the synthetic input
    (rolling mean, rolling stddev, z-score) and asserts the rule's
    output against that hand-derived expectation. Severity bucket
    boundaries (2.5 / 3 / 4) are pinned by their own test.
    """

    def test_exactly_three_stddevs_fires_warning_with_score_three(
        self, detection_rules_config,
    ):
        """A day whose value sits exactly 3 stddevs above the trailing
        28-day mean fires one warning flag with severity_score 3.0 and
        expected_low/high equal to that hand-computed mean.

        Business-correctness: rolling mean and stddev are computed in
        the test from the prior 28 alternating-value rows, so the
        expected score is hand-derived and not snapshotted.
        """
        sales, expected_mean, expected_std = _alternating_with_target(
            center=1000.0, half=100.0, target_z=3.0,
        )
        target = sales[-1]
        rows = _zscore_rows(1, sales)
        cfg = _only_zscore_config(detection_rules_config)

        result = detect_rules.run_all_rules(
            _metrics_df(rows), _zscore_dim_stores([1]), cfg,
        )

        z_flags = result[result["rule_id"] == "revenue_zscore_28d"]
        assert len(z_flags) == 1
        flag = z_flags.iloc[0]
        assert flag["severity_level"] == "warning"
        assert round(float(flag["severity_score"]), 2) == 3.0
        assert flag["expected_low"] == pytest.approx(expected_mean)
        assert flag["expected_high"] == pytest.approx(expected_mean)
        assert flag["actual_value"] == pytest.approx(target)
        assert flag["distance_from_band"] == pytest.approx(
            abs(target - expected_mean)
        )

    def test_insufficient_history_skipped(self, detection_rules_config):
        """A 14-row store fires nothing: day 14 has only 13 prior rows,
        below the 14-prior-day minimum the rule documents.

        Business-correctness: 13 < 14 → the rolling window is below
        min_periods → the row's rolling mean is NaN → the rule has no
        baseline to compare against and emits no flag.
        """
        sales = [1000.0] * 13 + [100000.0]  # day 14 wildly anomalous
        rows = _zscore_rows(1, sales)
        cfg = _only_zscore_config(detection_rules_config)

        result = detect_rules.run_all_rules(
            _metrics_df(rows), _zscore_dim_stores([1]), cfg,
        )

        assert (result["rule_id"] == "revenue_zscore_28d").sum() == 0

    def test_zero_stddev_history_skipped(self, detection_rules_config):
        """A flat 28-day history followed by a different value on day
        29 fires nothing — z-score is undefined when the prior stddev
        is zero, and the rule guards against the divide-by-zero rather
        than emitting an infinite-severity flag.
        """
        sales = [500.0] * 28 + [9999.0]
        rows = _zscore_rows(1, sales)
        cfg = _only_zscore_config(detection_rules_config)

        result = detect_rules.run_all_rules(
            _metrics_df(rows), _zscore_dim_stores([1]), cfg,
        )

        assert (result["rule_id"] == "revenue_zscore_28d").sum() == 0

    def test_per_store_independent_baselines(self, detection_rules_config):
        """Two stores with independently anomalous final days each fire
        one flag, and each flag's expected baseline matches that
        store's own rolling history — not the other's.

        Business-correctness: per-store rolling mean and stddev are
        recomputed in the test from each store's own prior window, so
        the expected_value asserts independence rather than just
        flag count.
        """
        sales_a, mean_a, _ = _alternating_with_target(
            center=1000.0, half=100.0, target_z=3.5,
        )
        sales_b, mean_b, _ = _alternating_with_target(
            center=500.0, half=50.0, target_z=-3.5,
        )

        rows_a = _zscore_rows(1, sales_a)
        rows_b = _zscore_rows(2, sales_b)
        cfg = _only_zscore_config(detection_rules_config)

        result = detect_rules.run_all_rules(
            _metrics_df(rows_a + rows_b),
            _zscore_dim_stores([1, 2]),
            cfg,
        )

        z_flags = result[result["rule_id"] == "revenue_zscore_28d"]
        assert len(z_flags) == 2

        flag_a = z_flags[z_flags["store_id"] == 1].iloc[0]
        flag_b = z_flags[z_flags["store_id"] == 2].iloc[0]
        assert flag_a["expected_low"] == pytest.approx(mean_a)
        assert flag_b["expected_low"] == pytest.approx(mean_b)
        assert flag_a["expected_low"] != pytest.approx(flag_b["expected_low"])

    @pytest.mark.parametrize(
        "z_multiple, expected_severity",
        [(2.6, "info"), (3.5, "warning"), (4.5, "critical")],
    )
    def test_severity_bucket_boundaries(
        self, detection_rules_config, z_multiple, expected_severity,
    ):
        """The 2.5 / 3 / 4 cutoffs each land in the right bucket.

        Business-correctness: target is computed as
        `mean + z_multiple * stddev` and the rule's reported severity
        is compared against the bucket the cutoff lookup defines.
        """
        sales, _, _ = _alternating_with_target(
            center=1000.0, half=100.0, target_z=z_multiple,
        )
        rows = _zscore_rows(1, sales)
        cfg = _only_zscore_config(detection_rules_config)

        result = detect_rules.run_all_rules(
            _metrics_df(rows), _zscore_dim_stores([1]), cfg,
        )

        z_flags = result[result["rule_id"] == "revenue_zscore_28d"]
        assert len(z_flags) == 1
        assert z_flags.iloc[0]["severity_level"] == expected_severity
        assert float(z_flags.iloc[0]["severity_score"]) == pytest.approx(
            z_multiple, abs=1e-6
        )
