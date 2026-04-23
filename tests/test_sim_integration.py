"""Integration tests: sim_ingest + sim_transform composed against real fixtures.

No new logic under test here — these tests prove the two modules
cooperate correctly across every acceptance scenario before the CLI
layer (commit 5) is wired.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src import sim_ingest, sim_transform
from src.exceptions import ReconciliationError, SchemaValidationError
from src.schemas import STORE_DAILY_METRICS_COLUMNS


def test_happy_path_row_count_matches_input(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dim_stores = sim_ingest.load_dim_stores(sim_happy_root)
    result = sim_transform.build_store_daily_metrics(records, dim_stores)
    assert len(result) == len(records) == 24


def test_happy_path_columns_in_target_order(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dim_stores = sim_ingest.load_dim_stores(sim_happy_root)
    result = sim_transform.build_store_daily_metrics(records, dim_stores)
    assert tuple(result.columns) == STORE_DAILY_METRICS_COLUMNS


def test_happy_path_sorted_by_date_then_store(sim_happy_root):
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dim_stores = sim_ingest.load_dim_stores(sim_happy_root)
    result = sim_transform.build_store_daily_metrics(records, dim_stores)
    for i in range(1, len(result)):
        prev, curr = result.iloc[i - 1], result.iloc[i]
        assert (prev["date"], prev["store_id"]) <= (curr["date"], curr["store_id"])


def test_happy_path_idempotent_across_runs(sim_happy_root):
    """Running the full pipeline twice must produce .equals() DataFrames."""
    first_records = list(sim_ingest.load_store_summaries(sim_happy_root))
    first_dim = sim_ingest.load_dim_stores(sim_happy_root)
    first = sim_transform.build_store_daily_metrics(first_records, first_dim)

    second_records = list(sim_ingest.load_store_summaries(sim_happy_root))
    second_dim = sim_ingest.load_dim_stores(sim_happy_root)
    second = sim_transform.build_store_daily_metrics(second_records, second_dim)

    assert first.equals(second)


def test_corrupt_fixture_raises_before_transform(sim_corrupt_root):
    """Schema failure during adapter read must prevent the transform entirely."""
    with pytest.raises(SchemaValidationError):
        list(sim_ingest.load_store_summaries(sim_corrupt_root))


def test_partial_fixture_raises_reconciliation_error(sim_partial_root):
    """Missing store_summary.csv in a walked date dir is a reconciliation error."""
    with pytest.raises(ReconciliationError):
        list(sim_ingest.load_store_summaries(sim_partial_root))


def test_orphan_store_id_detected_during_transform(sim_happy_root):
    """Happy records + a dim_stores table missing a store_id must fail validation."""
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dim_stores_missing_8 = pd.DataFrame({"store_id": list(range(1, 8))})
    with pytest.raises(SchemaValidationError) as excinfo:
        sim_transform.build_store_daily_metrics(records, dim_stores_missing_8)
    assert "8" in str(excinfo.value)


def test_happy_avg_basket_reasonable_range(sim_happy_root):
    """Spot check: no avg_basket_size values are negative or absurdly large."""
    records = list(sim_ingest.load_store_summaries(sim_happy_root))
    dim_stores = sim_ingest.load_dim_stores(sim_happy_root)
    result = sim_transform.build_store_daily_metrics(records, dim_stores)
    finite = result["avg_basket_size"].dropna()
    assert (finite > 0).all()
    assert (finite < 10000).all()
