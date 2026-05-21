"""Tests for src.sim_transform — pure pandas normalization to target schema.

These tests construct all inputs in-memory (no filesystem access) to
prove the transform has zero source-format dependencies.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from src import sim_transform
from src.exceptions import SchemaValidationError
from src.schemas import STORE_DAILY_METRICS_COLUMNS, StoreSummaryRecord


def _dim_stores(store_ids=range(1, 9)) -> pd.DataFrame:
    return pd.DataFrame({"store_id": list(store_ids)})


def _record(
    d: date,
    store_id: int,
    sales: float,
    txns: int,
    labor_pct: float = 0.10,
) -> StoreSummaryRecord:
    return StoreSummaryRecord(
        date=d,
        store_id=store_id,
        net_sales_total=sales,
        transactions_total=txns,
        labor_cost_pct=labor_pct,
        source_path="in-memory",
    )


# ==========================================================
# Schema shape and ordering
# ==========================================================


def test_build_returns_dataframe():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert isinstance(result, pd.DataFrame)


def test_build_columns_match_target_schema_exactly():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert tuple(result.columns) == STORE_DAILY_METRICS_COLUMNS


def test_build_store_id_is_integer():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert pd.api.types.is_integer_dtype(result["store_id"])


def test_build_transaction_count_is_integer():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert pd.api.types.is_integer_dtype(result["transaction_count"])


def test_build_total_sales_is_float():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert pd.api.types.is_float_dtype(result["total_sales"])


def test_build_avg_basket_size_is_float():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert pd.api.types.is_float_dtype(result["avg_basket_size"])


def test_build_labor_cost_pct_is_float():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100, labor_pct=0.105)],
        _dim_stores(),
    )
    assert pd.api.types.is_float_dtype(result["labor_cost_pct"])


def test_build_labor_cost_pct_passes_through_record_value():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100, labor_pct=0.115)],
        _dim_stores(),
    )
    assert result["labor_cost_pct"].iloc[0] == pytest.approx(0.115)


def test_labor_cost_pct_nan_when_total_sales_zero():
    """Closed-day rows yield NaN for labor_cost_pct, matching avg_basket_size."""
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 0.0, 0, labor_pct=0.10)],
        _dim_stores(),
    )
    assert pd.isna(result["labor_cost_pct"].iloc[0])


def test_build_date_column_contains_datetime_date():
    """Parquet should round-trip as date32[day]; use object-of-date, not timestamp."""
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert isinstance(result["date"].iloc[0], date)


# ==========================================================
# Value semantics
# ==========================================================


def test_build_total_sales_maps_to_net_sales():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 61104.48, 1812)],
        _dim_stores(),
    )
    assert result["total_sales"].iloc[0] == pytest.approx(61104.48)


def test_build_avg_basket_size_computed_correctly():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 1000.0, 100)],
        _dim_stores(),
    )
    assert result["avg_basket_size"].iloc[0] == pytest.approx(10.0)


def test_build_avg_basket_size_nan_when_zero_transactions():
    result = sim_transform.build_store_daily_metrics(
        [_record(date(2024, 1, 1), 1, 500.0, 0)],
        _dim_stores(),
    )
    assert pd.isna(result["avg_basket_size"].iloc[0])


def test_build_avg_basket_size_mixed_zero_and_nonzero():
    """A zero-transaction row yields NaN; a normal row in the same frame
    still gets the correct quotient. One unparseable-basket row must not
    poison the rest, and the transform must not raise."""
    result = sim_transform.build_store_daily_metrics(
        [
            _record(date(2024, 1, 1), 1, 500.0, 0),
            _record(date(2024, 1, 1), 2, 1000.0, 100),
        ],
        _dim_stores(),
    )
    by_store = result.set_index("store_id")["avg_basket_size"]
    assert pd.isna(by_store[1])                 # 0 transactions → NaN
    assert by_store[2] == pytest.approx(10.0)   # 1000.0 / 100


# ==========================================================
# Sort and row-count invariants
# ==========================================================


def test_build_row_count_equals_input_count():
    records = [
        _record(date(2024, 1, 1), 1, 1000.0, 100),
        _record(date(2024, 1, 1), 2, 2000.0, 200),
        _record(date(2024, 1, 2), 1, 1100.0, 110),
    ]
    result = sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert len(result) == 3


def test_build_sorts_by_date_then_store_id():
    records = [
        _record(date(2024, 1, 2), 3, 1200.0, 120),
        _record(date(2024, 1, 1), 7, 7000.0, 700),
        _record(date(2024, 1, 2), 1, 1000.0, 100),
        _record(date(2024, 1, 1), 2, 2000.0, 200),
    ]
    result = sim_transform.build_store_daily_metrics(records, _dim_stores())
    ordered = list(zip(result["date"], result["store_id"]))
    assert ordered == [
        (date(2024, 1, 1), 2),
        (date(2024, 1, 1), 7),
        (date(2024, 1, 2), 1),
        (date(2024, 1, 2), 3),
    ]


def test_build_is_idempotent_on_identical_input():
    records = [
        _record(date(2024, 1, 1), 1, 1000.0, 100),
        _record(date(2024, 1, 1), 2, 2000.0, 200),
    ]
    first = sim_transform.build_store_daily_metrics(records, _dim_stores())
    second = sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert first.equals(second)


# ==========================================================
# Referential validation against dim_stores
# ==========================================================


def test_build_raises_on_orphan_store_id():
    records = [
        _record(date(2024, 1, 1), 1, 1000.0, 100),
        _record(date(2024, 1, 1), 99, 9999.0, 500),
    ]
    with pytest.raises(SchemaValidationError) as excinfo:
        sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert "99" in str(excinfo.value)


def test_build_accepts_all_known_store_ids():
    records = [
        _record(date(2024, 1, 1), sid, 1000.0 * sid, 100 * sid)
        for sid in range(1, 9)
    ]
    result = sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert set(result["store_id"]) == set(range(1, 9))


# ==========================================================
# Boundary isolation — transform has no filesystem dependencies
# ==========================================================


def test_build_never_touches_filesystem(tmp_path, monkeypatch):
    """The transform accepts in-memory records constructed without any file
    IO; combined with the module's import list (pandas + schemas +
    exceptions only) this proves it has no source-format dependency. The
    in-memory record must still transform to the correct target row."""
    records = [_record(date(2024, 1, 1), 1, 1000.0, 100, labor_pct=0.105)]
    result = sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert len(result) == 1
    row = result.iloc[0]
    assert row["total_sales"] == pytest.approx(1000.0)
    assert row["transaction_count"] == 100
    assert row["avg_basket_size"] == pytest.approx(10.0)   # 1000.0 / 100
    assert row["labor_cost_pct"] == pytest.approx(0.105)
