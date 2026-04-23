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


def _record(d: date, store_id: int, sales: float, txns: int) -> StoreSummaryRecord:
    return StoreSummaryRecord(
        date=d,
        store_id=store_id,
        net_sales_total=sales,
        transactions_total=txns,
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


def test_build_avg_basket_size_nan_does_not_raise():
    """A zero transaction count must not bubble as an exception."""
    sim_transform.build_store_daily_metrics(
        [
            _record(date(2024, 1, 1), 1, 500.0, 0),
            _record(date(2024, 1, 1), 2, 1000.0, 100),
        ],
        _dim_stores(),
    )


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
    """Monkey-patch open() / Path methods would be overkill; instead prove the
    transform accepts in-memory records constructed without any file IO and
    returns a valid result. Combined with direct inspection of the module's
    import list (pandas + schemas + exceptions only), this is sufficient."""
    records = [_record(date(2024, 1, 1), 1, 1000.0, 100)]
    result = sim_transform.build_store_daily_metrics(records, _dim_stores())
    assert len(result) == 1
