"""Tests for src.detect_rules — pure-pandas exception detection rules.

Every rule is exercised in isolation with synthetic in-memory frames so
band edges, severity buckets, and skip behaviors are pinned without
involvement from the YAML loader or the parquet IO layer.
"""

from __future__ import annotations

from datetime import date
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
        "  department_coverage: { enabled: false }\n"
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
    # base 95000, lower band 71250, half_width 23750. 1% past edge → info.
    rows = [_metric_row(date(2024, 6, 15), 1, 71000.0, 1868, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "revenue_band"]
    assert len(flags) == 1
    assert flags["severity_level"].iloc[0] == "info"


def test_revenue_band_far_outside_critical_severity(
    sample_dim_stores_df, detection_rules_config
):
    # base 95000, lower band 71250, half_width 23750. score > 2 needs distance > 47500
    # so total_sales < 23750. Use 10000.
    rows = [_metric_row(date(2024, 6, 15), 1, 10000.0, 263, 0.105)]
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
    # base 95000, upper band 118750. 130000 is above → fires.
    rows = [_metric_row(date(2024, 6, 15), 1, 130000.0, 3421, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flags = result[result["rule_id"] == "revenue_band"]
    assert len(flags) == 1
    assert flags["actual_value"].iloc[0] == pytest.approx(130000.0)
    assert flags["distance_from_band"].iloc[0] == pytest.approx(11250.0)


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
    # store 7: base 55000 / avg_ticket 32 = 1718.75; band [1289, 2148].
    rows = [_metric_row(date(2024, 6, 15), 7, 50600.0, 1581, 0.120)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    assert (result["rule_id"] == "transactions_band").sum() == 0


def test_transactions_band_below_lower_edge_fires(
    sample_dim_stores_df, detection_rules_config
):
    # store 7 expected 1718.75, band [1289, 2148]. 600 way below.
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
    """Prior 100000, current 50000 → ratio 0.5 < 0.85."""
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
    """Prior 50000, current 100000 → ratio 2.0 > 1.25."""
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
# severity scoring math
# ==============================================================================


def test_severity_score_equals_distance_over_band_half_width(
    sample_dim_stores_df, detection_rules_config
):
    # Construct revenue_band breach with known math.
    # base 95000, band [71250, 118750], half_width 23750.
    # actual 50000 → distance 21250 → score 21250/23750 = 0.8947
    rows = [_metric_row(date(2024, 6, 15), 1, 50000.0, 1316, 0.105)]
    result = detect_rules.run_all_rules(
        _metrics_df(rows), sample_dim_stores_df, detection_rules_config
    )
    flag = result[result["rule_id"] == "revenue_band"].iloc[0]
    assert flag["distance_from_band"] == pytest.approx(21250.0)
    assert flag["severity_score"] == pytest.approx(21250.0 / 23750.0)
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
