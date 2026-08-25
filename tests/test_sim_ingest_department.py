"""Tests for the department-grain source adapter.

Exercises the tree walker, CSV parsing, typed record shape, and failure
modes against the on-disk fixtures under tests/fixtures/sim_engine/ as
well as small inline trees built in tmp_path for the negative cases.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from src import sim_ingest
from src.exceptions import ReconciliationError, SchemaValidationError
from src.schemas import DepartmentSalesRecord


# ==========================================================
# load_department_sales â€” happy path against shared fixture
# ==========================================================


def test_returns_iterable_of_records(sim_happy_root):
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    assert records
    assert all(isinstance(r, DepartmentSalesRecord) for r in records)


def test_total_row_count(sim_happy_root):
    """3 dates x 8 stores x 2 departments = 48 records across the fixture."""
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    assert len(records) == 48


def test_field_types(sim_happy_root):
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    first = records[0]
    assert isinstance(first.date, date)
    assert isinstance(first.store_id, int)
    assert isinstance(first.department_id, int)
    assert isinstance(first.net_sales, float)
    assert isinstance(first.transactions, int)
    assert isinstance(first.units_sold, int)
    assert isinstance(first.gross_margin_pct, float)
    assert isinstance(first.source_path, str)


def test_covers_all_three_dates(sim_happy_root):
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    dates = {r.date for r in records}
    assert dates == {date(2024, 6, 15), date(2024, 6, 16), date(2024, 6, 17)}


def test_covers_all_eight_stores_and_two_departments(sim_happy_root):
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    assert {r.store_id for r in records} == set(range(1, 9))
    assert {r.department_id for r in records} == {1, 2}


def test_deterministic_yield_order(sim_happy_root):
    """Repeated invocations on identical input produce identical sequences."""
    first = list(sim_ingest.load_department_sales(sim_happy_root))
    second = list(sim_ingest.load_department_sales(sim_happy_root))
    assert [(r.date, r.store_id, r.department_id) for r in first] == [
        (r.date, r.store_id, r.department_id) for r in second
    ]


def test_source_path_points_at_csv(sim_happy_root):
    records = list(sim_ingest.load_department_sales(sim_happy_root))
    paths = {Path(r.source_path) for r in records}
    assert len(paths) == 3  # three distinct department_sales.csv files
    for p in paths:
        assert p.name == "department_sales.csv"
        assert p.exists()


# ==========================================================
# load_department_sales â€” failure modes
# ==========================================================


def test_missing_daily_subtree_raises(tmp_path):
    with pytest.raises(ReconciliationError) as excinfo:
        list(sim_ingest.load_department_sales(tmp_path))
    assert "daily" in str(excinfo.value)


def test_empty_daily_tree_raises(tmp_path):
    """daily/ exists but contains no date directories."""
    (tmp_path / "daily").mkdir()
    with pytest.raises(ReconciliationError):
        list(sim_ingest.load_department_sales(tmp_path))


def test_missing_csv_in_walked_date_dir_raises(tmp_path):
    """A walked date directory missing department_sales.csv is a reconciliation error."""
    date_dir = tmp_path / "daily" / "2025" / "07" / "01"
    date_dir.mkdir(parents=True)
    # store_summary present, department_sales absent
    (date_dir / "store_summary.csv").write_text(
        "date_key,store_id,net_sales_total,transactions_total,labor_cost_pct\n",
        encoding="utf-8",
    )
    with pytest.raises(ReconciliationError) as excinfo:
        list(sim_ingest.load_department_sales(tmp_path))
    assert "department_sales.csv" in str(excinfo.value)


def test_missing_required_column_raises(tmp_path):
    date_dir = tmp_path / "daily" / "2025" / "07" / "01"
    date_dir.mkdir(parents=True)
    (date_dir / "department_sales.csv").write_text(
        "date_key,store_id,net_sales,transactions,units_sold,gross_margin_pct\n"
        "2025-07-01,1,1000.0,42,120,0.35\n",
        encoding="utf-8",
    )
    with pytest.raises(SchemaValidationError) as excinfo:
        list(sim_ingest.load_department_sales(tmp_path))
    assert "department_id" in str(excinfo.value)


def test_unparseable_row_raises(tmp_path):
    date_dir = tmp_path / "daily" / "2025" / "07" / "01"
    date_dir.mkdir(parents=True)
    (date_dir / "department_sales.csv").write_text(
        "date_key,store_id,department_id,net_sales,transactions,units_sold,gross_margin_pct\n"
        "2025-07-01,not-a-number,1,1000.0,42,120,0.35\n",
        encoding="utf-8",
    )
    with pytest.raises(SchemaValidationError):
        list(sim_ingest.load_department_sales(tmp_path))
