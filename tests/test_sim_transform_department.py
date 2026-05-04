"""Tests for src.sim_transform.build_department_daily_metrics.

These tests construct all inputs in-memory (no filesystem access) to
prove the transform has zero source-format dependencies.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src import sim_transform
from src.exceptions import ReconciliationError, SchemaValidationError
from src.schemas import DEPARTMENT_DAILY_METRICS_COLUMNS, DepartmentSalesRecord


def _dim_stores(store_ids=range(1, 9)) -> pd.DataFrame:
    return pd.DataFrame({"store_id": list(store_ids)})


def _record(
    d: date,
    store_id: int,
    department_id: int,
    net_sales: float = 1000.0,
    transactions: int = 42,
    units_sold: int = 120,
    gross_margin_pct: float = 0.35,
) -> DepartmentSalesRecord:
    return DepartmentSalesRecord(
        date=d,
        store_id=store_id,
        department_id=department_id,
        net_sales=net_sales,
        transactions=transactions,
        units_sold=units_sold,
        gross_margin_pct=gross_margin_pct,
        source_path="in-memory",
    )


# ==========================================================
# Schema shape and ordering
# ==========================================================


def test_build_returns_dataframe():
    result = sim_transform.build_department_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1)],
        _dim_stores(),
    )
    assert isinstance(result, pd.DataFrame)


def test_build_columns_match_target_schema_exactly():
    result = sim_transform.build_department_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1)],
        _dim_stores(),
    )
    assert tuple(result.columns) == DEPARTMENT_DAILY_METRICS_COLUMNS


def test_build_dtypes():
    result = sim_transform.build_department_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1)],
        _dim_stores(),
    )
    assert pd.api.types.is_integer_dtype(result["store_id"])
    assert pd.api.types.is_integer_dtype(result["department_id"])
    assert pd.api.types.is_integer_dtype(result["transactions"])
    assert pd.api.types.is_integer_dtype(result["units_sold"])
    assert pd.api.types.is_float_dtype(result["net_sales"])
    assert pd.api.types.is_float_dtype(result["gross_margin_pct"])


def test_build_date_column_contains_datetime_date():
    result = sim_transform.build_department_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1)],
        _dim_stores(),
    )
    assert isinstance(result["date"].iloc[0], date)


# ==========================================================
# Value pass-through
# ==========================================================


def test_values_pass_through_unchanged():
    record = _record(
        date(2025, 7, 1), 1, 1,
        net_sales=12345.67, transactions=200, units_sold=550,
        gross_margin_pct=0.42,
    )
    result = sim_transform.build_department_daily_metrics([record], _dim_stores())
    row = result.iloc[0]
    assert row["date"] == date(2025, 7, 1)
    assert row["store_id"] == 1
    assert row["department_id"] == 1
    assert row["net_sales"] == pytest.approx(12345.67)
    assert row["transactions"] == 200
    assert row["units_sold"] == 550
    assert row["gross_margin_pct"] == pytest.approx(0.42)


def test_gross_margin_pct_stays_a_fraction():
    """The transform never converts 0.35 to 35; consumers handle display."""
    result = sim_transform.build_department_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1, gross_margin_pct=0.35)],
        _dim_stores(),
    )
    assert result["gross_margin_pct"].iloc[0] == pytest.approx(0.35)


# ==========================================================
# Sort and row-count invariants
# ==========================================================


def test_row_count_equals_input_count():
    records = [
        _record(date(2024, 1, 1), 1, 1),
        _record(date(2024, 1, 1), 1, 2),
        _record(date(2024, 1, 2), 2, 1),
    ]
    result = sim_transform.build_department_daily_metrics(records, _dim_stores())
    assert len(result) == 3


def test_sorts_by_date_then_store_id_then_department_id():
    records = [
        _record(date(2024, 1, 2), 3, 2),
        _record(date(2024, 1, 1), 7, 1),
        _record(date(2024, 1, 2), 1, 1),
        _record(date(2024, 1, 1), 2, 2),
        _record(date(2024, 1, 1), 2, 1),
    ]
    result = sim_transform.build_department_daily_metrics(records, _dim_stores())
    ordered = list(zip(result["date"], result["store_id"], result["department_id"]))
    assert ordered == [
        (date(2024, 1, 1), 2, 1),
        (date(2024, 1, 1), 2, 2),
        (date(2024, 1, 1), 7, 1),
        (date(2024, 1, 2), 1, 1),
        (date(2024, 1, 2), 3, 2),
    ]


def test_idempotent_on_identical_input():
    records = [
        _record(date(2024, 1, 1), 1, 1),
        _record(date(2024, 1, 1), 1, 2),
        _record(date(2024, 1, 1), 2, 1),
    ]
    first = sim_transform.build_department_daily_metrics(records, _dim_stores())
    second = sim_transform.build_department_daily_metrics(records, _dim_stores())
    assert first.equals(second)


# ==========================================================
# Failure modes
# ==========================================================


def test_empty_records_raises_reconciliation_error():
    """Empty input signals the upstream walker found no dept files."""
    with pytest.raises(ReconciliationError):
        sim_transform.build_department_daily_metrics([], _dim_stores())


def test_orphan_store_id_raises_schema_validation_error():
    """Mirrors build_store_daily_metrics: orphan store_ids are a schema error."""
    records = [
        _record(date(2024, 1, 1), 1, 1),
        _record(date(2024, 1, 1), 99, 2),
    ]
    with pytest.raises(SchemaValidationError) as excinfo:
        sim_transform.build_department_daily_metrics(records, _dim_stores())
    assert "99" in str(excinfo.value)


def test_accepts_all_known_store_ids():
    records = [
        _record(date(2024, 1, 1), sid, dept)
        for sid in range(1, 9)
        for dept in (1, 2)
    ]
    result = sim_transform.build_department_daily_metrics(records, _dim_stores())
    assert set(result["store_id"]) == set(range(1, 9))
    assert set(result["department_id"]) == {1, 2}
    assert len(result) == 16
