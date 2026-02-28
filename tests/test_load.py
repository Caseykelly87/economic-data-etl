import pandas as pd
import pytest
from sqlalchemy import inspect, text
from src import load


# ==========================================================
# Table Creation Tests
# Function under test: load.ensure_tables_exist(engine)
# ==========================================================

def test_ensure_tables_creates_observations_table(db_engine):
    load.ensure_tables_exist(db_engine)
    assert inspect(db_engine).has_table("fact_economic_observations")


def test_ensure_tables_creates_dim_table(db_engine):
    load.ensure_tables_exist(db_engine)
    assert inspect(db_engine).has_table("dim_series")


def test_ensure_tables_is_idempotent(db_engine):
    """Calling twice must not raise — tables use CREATE IF NOT EXISTS semantics."""
    load.ensure_tables_exist(db_engine)
    load.ensure_tables_exist(db_engine)


# ==========================================================
# upsert_observations — initial insert behaviour
# Function under test: load.upsert_observations(df, engine) -> dict
# ==========================================================

def test_upsert_observations_inserts_all_rows(db_engine, sample_observations_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_economic_observations")).scalar()
    assert count == 3


def test_upsert_observations_returns_inserted_count(db_engine, sample_observations_df):
    load.ensure_tables_exist(db_engine)
    result = load.upsert_observations(sample_observations_df, db_engine)
    assert result["inserted"] == 3


def test_upsert_observations_first_run_zero_updated(db_engine, sample_observations_df):
    load.ensure_tables_exist(db_engine)
    result = load.upsert_observations(sample_observations_df, db_engine)
    assert result["updated"] == 0


def test_upsert_observations_correct_value_in_db(db_engine, sample_observations_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM fact_economic_observations WHERE series_id = 'FEDFUNDS'", conn)
    assert pytest.approx(df["value"].iloc[0], abs=0.01) == 5.33


def test_upsert_observations_nan_persisted_as_null(db_engine, sample_observations_df):
    """NaN in the DataFrame must become NULL in the database, not the string 'nan'."""
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM fact_economic_observations WHERE series_id = 'UNRATE'", conn)

    feb_row = df[df["date"] == pd.Timestamp("2024-02-01")]
    assert feb_row["value"].isna().all()


# ==========================================================
# upsert_observations — upsert / idempotency behaviour
# ==========================================================

def test_upsert_observations_no_duplicates_on_identical_rerun(db_engine, sample_observations_df):
    """Identical second run must not increase the row count."""
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_economic_observations")).scalar()
    assert count == 3


def test_upsert_observations_unchanged_count_on_identical_rerun(db_engine, sample_observations_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)
    result = load.upsert_observations(sample_observations_df, db_engine)
    assert result["unchanged"] == 3
    assert result["inserted"] == 0


def test_upsert_observations_updates_changed_value(db_engine, sample_observations_df):
    """A row with the same (series_id, date) key but a new value must be updated in place."""
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    revised = sample_observations_df.copy()
    revised.loc[revised["series_id"] == "FEDFUNDS", "value"] = 5.50
    load.upsert_observations(revised, db_engine)

    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM fact_economic_observations WHERE series_id = 'FEDFUNDS'", conn)
    assert pytest.approx(df["value"].iloc[0], abs=0.01) == 5.50


def test_upsert_observations_returns_correct_stats_on_partial_update(db_engine, sample_observations_df):
    """1 changed row → updated=1, unchanged=2, inserted=0."""
    load.ensure_tables_exist(db_engine)
    load.upsert_observations(sample_observations_df, db_engine)

    revised = sample_observations_df.copy()
    revised.loc[revised["series_id"] == "FEDFUNDS", "value"] = 5.50
    result = load.upsert_observations(revised, db_engine)

    assert result["updated"]   == 1
    assert result["unchanged"] == 2
    assert result["inserted"]  == 0


# ==========================================================
# upsert_dim_series Tests
# Function under test: load.upsert_dim_series(df, engine) -> dict
# ==========================================================

def test_upsert_dim_series_inserts_rows(db_engine, sample_dim_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_dim_series(sample_dim_df, db_engine)

    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dim_series")).scalar()
    assert count == 3


def test_upsert_dim_series_returns_inserted_count(db_engine, sample_dim_df):
    load.ensure_tables_exist(db_engine)
    result = load.upsert_dim_series(sample_dim_df, db_engine)
    assert result["inserted"] == 3


def test_upsert_dim_series_no_duplicates_on_rerun(db_engine, sample_dim_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_dim_series(sample_dim_df, db_engine)
    load.upsert_dim_series(sample_dim_df, db_engine)

    with db_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM dim_series")).scalar()
    assert count == 3


def test_upsert_dim_series_unchanged_on_identical_rerun(db_engine, sample_dim_df):
    load.ensure_tables_exist(db_engine)
    load.upsert_dim_series(sample_dim_df, db_engine)
    result = load.upsert_dim_series(sample_dim_df, db_engine)
    assert result["unchanged"] == 3
    assert result["inserted"]  == 0