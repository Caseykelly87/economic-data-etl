"""Tests for src.sim_ingest — sim engine source adapter.

Exercises the tree walker, CSV parsing, typed record shape, and failure
modes against the on-disk fixtures under tests/fixtures/sim_engine/.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src import sim_ingest
from src.exceptions import ReconciliationError, SchemaValidationError
from src.schemas import StoreSummaryRecord


# ==========================================================
# load_store_summaries — happy path
# ==========================================================


def test_load_store_summaries_returns_iterable_of_records(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    assert records
    assert all(isinstance(r, StoreSummaryRecord) for r in records)


def test_load_store_summaries_total_row_count(sim_happy_root):
    """3 dates x 8 stores = 24 records across every store_summary.csv."""
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    assert len(records) == 24


def test_load_store_summaries_field_types(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    first = records[0]
    assert isinstance(first.date, date)
    assert isinstance(first.store_id, int)
    assert isinstance(first.net_sales_total, float)
    assert isinstance(first.transactions_total, int)
    assert isinstance(first.source_path, str)


def test_load_store_summaries_source_path_points_at_csv(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    paths = {Path(r.source_path) for r in records}
    assert len(paths) == 3  # three distinct store_summary.csv files
    for p in paths:
        assert p.name == "store_summary.csv"
        assert p.exists()


def test_load_store_summaries_covers_all_three_dates(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dates = {r.date for r in records}
    assert dates == {date(2024, 6, 15), date(2024, 6, 16), date(2024, 6, 17)}


def test_load_store_summaries_covers_all_eight_stores(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    store_ids = {r.store_id for r in records}
    assert store_ids == set(range(1, 9))


def test_load_store_summaries_values_nonzero(sim_happy_root):
    """Spot-check a known row — store 1 on 2024-06-15 has net_sales_total 87400.00."""
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    match = [
        r for r in records
        if r.date == date(2024, 6, 15) and r.store_id == 1
    ]
    assert len(match) == 1
    assert match[0].net_sales_total == pytest.approx(87400.00)
    assert match[0].transactions_total == 2300


# ==========================================================
# load_store_summaries — failure modes
# ==========================================================


def test_load_store_summaries_missing_column_raises(sim_corrupt_root):
    """Missing net_sales_total must raise SchemaValidationError citing the file."""
    with pytest.raises(SchemaValidationError) as excinfo:
        list(sim_ingest.load_store_summaries(sim_corrupt_root))
    msg = str(excinfo.value)
    assert "net_sales_total" in msg
    assert "store_summary.csv" in msg


def test_load_store_summaries_missing_file_raises(sim_partial_root):
    """A walked date directory without store_summary.csv must raise ReconciliationError."""
    with pytest.raises(ReconciliationError) as excinfo:
        list(sim_ingest.load_store_summaries(sim_partial_root))
    msg = str(excinfo.value)
    assert "06" in msg and "17" in msg and "2024" in msg


def test_load_store_summaries_empty_tree_raises(tmp_path):
    """No date directories at all is a reconciliation failure (not zero rows)."""
    (tmp_path / "daily").mkdir()
    with pytest.raises(ReconciliationError):
        list(sim_ingest.load_store_summaries(tmp_path))


def test_load_store_summaries_nonexistent_root_raises(tmp_path):
    """Missing daily/ subtree entirely is also a reconciliation failure."""
    with pytest.raises(ReconciliationError):
        list(sim_ingest.load_store_summaries(tmp_path / "does_not_exist"))


# ==========================================================
# load_dim_stores
# ==========================================================


def test_load_dim_stores_returns_dataframe(sim_happy_root):
    df = sim_ingest.load_dim_stores(sim_happy_root)
    assert isinstance(df, pd.DataFrame)


def test_load_dim_stores_has_store_id_int(sim_happy_root):
    df = sim_ingest.load_dim_stores(sim_happy_root)
    assert "store_id" in df.columns
    assert pd.api.types.is_integer_dtype(df["store_id"])


def test_load_dim_stores_row_count(sim_happy_root):
    df = sim_ingest.load_dim_stores(sim_happy_root)
    assert len(df) == 8
    assert set(df["store_id"]) == set(range(1, 9))


def test_load_dim_stores_missing_required_column_raises(tmp_path):
    """A dim_stores.csv missing store_id must raise SchemaValidationError."""
    dims = tmp_path / "dimensions"
    dims.mkdir()
    (dims / "dim_stores.csv").write_text(
        "store_name,city\n"
        "Knot Shore Downtown,Saint Louis\n",
        encoding="utf-8",
    )
    with pytest.raises(SchemaValidationError) as excinfo:
        sim_ingest.load_dim_stores(tmp_path)
    assert "store_id" in str(excinfo.value)
