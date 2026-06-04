"""Behavior tests for the staging → marts build (src/marts.py).

These are transform-correctness tests: given known raw observations, they
assert each mart contains exactly the expected rows (right series in the
right mart, right date typing), that no ingested series is silently lost,
that the build is idempotent, and that the produced columns and primary keys
match the API's SQLAlchemy mart models so the API can read them.
"""

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from src import load, marts
from src.config import BLS_SERIES, FRED_SERIES, MART_DOMAINS


# A raw fixture spanning every domain plus unmapped series. series_id holds
# the technical ID (as raw.fact_economic_observations does); two dates per
# series so "latest" in the summary is a real choice, not the only row.
_RAW_ROWS = [
    # inflation
    ("CPIAUCSL", "CPI_ALL", "2024-01-01", 300.0, "FRED"),
    ("CPIAUCSL", "CPI_ALL", "2024-02-01", 301.5, "FRED"),
    ("CUUR0000SA0", "CPI_URBAN", "2024-01-01", 308.0, "BLS"),
    ("CUUR0000SA0", "CPI_URBAN", "2024-02-01", 309.2, "BLS"),
    ("APU000074714", "GAS_PRICE", "2024-01-01", 3.10, "BLS"),
    ("APU000074714", "GAS_PRICE", "2024-02-01", 3.25, "BLS"),
    # labor
    ("UNRATE", "UNRATE", "2024-01-01", 3.7, "FRED"),
    ("UNRATE", "UNRATE", "2024-02-01", 3.9, "FRED"),
    ("CES0500000003", "AVG_WAGES", "2024-01-01", 34.55, "BLS"),
    ("CES0500000003", "AVG_WAGES", "2024-02-01", 34.75, "BLS"),
    # gdp
    ("GDPC1", "GDP_REAL", "2023-10-01", 22000.0, "FRED"),
    ("GDPC1", "GDP_REAL", "2024-01-01", 22100.0, "FRED"),
    # unmapped (no domain mart) — must still reach the summary
    ("FEDFUNDS", "MONEY_COST", "2024-01-01", 5.33, "FRED"),
    ("FEDFUNDS", "MONEY_COST", "2024-02-01", 5.33, "FRED"),
    ("ERS_ALL_FOOD", "ERS_ALL_FOOD", "2024-01-01", 2.1, "ERS"),
]

# Hand-computed expectations derived directly from _RAW_ROWS above.
_DOMAIN_SERIES = {
    "mart_inflation": {"CPIAUCSL", "CUUR0000SA0", "APU000074714"},
    "mart_labor_market": {"UNRATE", "CES0500000003"},
    "mart_gdp": {"GDPC1"},
}
_ALL_RAW_SERIES = {row[0] for row in _RAW_ROWS}
_UNMAPPED_SERIES = {"FEDFUNDS", "ERS_ALL_FOOD"}


@pytest.fixture
def raw_loaded_engine(db_engine):
    """db_engine with raw tables created and _RAW_ROWS loaded into raw.fact."""
    load.ensure_tables_exist(db_engine)
    df = pd.DataFrame(
        _RAW_ROWS, columns=["series_id", "series_name", "date", "value", "source"]
    )
    df.to_sql(
        "fact_economic_observations",
        db_engine,
        schema="raw",
        if_exists="append",
        index=False,
    )
    return db_engine


def _rows(engine, table: str):
    with engine.connect() as conn:
        return pd.read_sql(f"SELECT * FROM {table}", conn)


# ==========================================================
# Schema / table creation
# ==========================================================

def test_build_marts_creates_staging_and_mart_tables(raw_loaded_engine):
    """Structural: build_marts must materialize the staging table and all
    four marts in their schemas so the warehouse chain physically exists."""
    marts.build_marts(raw_loaded_engine)
    insp = inspect(raw_loaded_engine)
    assert insp.has_table("stg_economic_observations", schema="staging")
    for table in ("mart_inflation", "mart_labor_market", "mart_gdp",
                  "mart_economic_summary"):
        assert insp.has_table(table, schema="public_analytics")


def test_ensure_tables_exist_attaches_all_schemas(db_engine):
    """Structural: ensure_tables_exist must make raw, staging, and
    public_analytics addressable, since marts target the latter two."""
    load.ensure_tables_exist(db_engine)
    with db_engine.connect() as conn:
        attached = {row[1] for row in conn.execute(text("PRAGMA database_list"))}
    assert {"raw", "staging", "public_analytics"} <= attached


# ==========================================================
# Transform correctness — domain routing
# ==========================================================

def test_each_domain_mart_holds_exactly_its_series(raw_loaded_engine):
    """business-correctness: each domain mart contains exactly the series_ids
    mapped to its domain in _RAW_ROWS — no more, no fewer."""
    marts.build_marts(raw_loaded_engine)
    for table, expected_ids in _DOMAIN_SERIES.items():
        rows = _rows(raw_loaded_engine, f"public_analytics.{table}")
        assert set(rows["series_id"]) == expected_ids


def test_domain_mart_row_count_matches_raw(raw_loaded_engine):
    """business-correctness: mart_inflation must carry every inflation
    observation (3 series × 2 dates = 6 rows), proving it projects all rows
    of its domain, not just the latest."""
    marts.build_marts(raw_loaded_engine)
    rows = _rows(raw_loaded_engine, "public_analytics.mart_inflation")
    assert len(rows) == 6


def test_cpi_lands_in_inflation_not_labor_or_gdp(raw_loaded_engine):
    """business-correctness: a CPI series routes to inflation and appears in
    no other domain mart — the routing is mutually exclusive."""
    marts.build_marts(raw_loaded_engine)
    assert "CPIAUCSL" in set(
        _rows(raw_loaded_engine, "public_analytics.mart_inflation")["series_id"]
    )
    assert "CPIAUCSL" not in set(
        _rows(raw_loaded_engine, "public_analytics.mart_labor_market")["series_id"]
    )
    assert "CPIAUCSL" not in set(
        _rows(raw_loaded_engine, "public_analytics.mart_gdp")["series_id"]
    )


def test_unmapped_series_in_no_domain_mart(raw_loaded_engine):
    """business-correctness: a series with no domain (the fed funds rate)
    must not appear in any of the three domain marts."""
    marts.build_marts(raw_loaded_engine)
    for table in _DOMAIN_SERIES:
        ids = set(_rows(raw_loaded_engine, f"public_analytics.{table}")["series_id"])
        assert _UNMAPPED_SERIES.isdisjoint(ids)


def test_domain_marts_partition_without_overlap(raw_loaded_engine):
    """business-correctness: no series_id appears in two domain marts — the
    domains partition the series they cover."""
    marts.build_marts(raw_loaded_engine)
    seen: set[str] = set()
    for table in _DOMAIN_SERIES:
        ids = set(_rows(raw_loaded_engine, f"public_analytics.{table}")["series_id"])
        assert seen.isdisjoint(ids), f"{table} overlaps an earlier domain mart"
        seen |= ids


# ==========================================================
# No-loss guarantee + summary correctness
# ==========================================================

def test_summary_holds_every_ingested_series(raw_loaded_engine):
    """business-correctness: mart_economic_summary carries one row for every
    distinct raw series_id — the serving layer loses no series, including the
    ones that map to no domain mart."""
    marts.build_marts(raw_loaded_engine)
    summary = _rows(raw_loaded_engine, "public_analytics.mart_economic_summary")
    assert set(summary["series_id"]) == _ALL_RAW_SERIES
    assert len(summary) == len(_ALL_RAW_SERIES)


def test_summary_reports_latest_observation(raw_loaded_engine):
    """business-correctness: the summary row for a multi-date series carries
    the value and date of its most recent observation, hand-computed from the
    fixture (CPIAUCSL: 2024-02-01 → 301.5; GDPC1: 2024-01-01 → 22100.0)."""
    marts.build_marts(raw_loaded_engine)
    summary = _rows(raw_loaded_engine, "public_analytics.mart_economic_summary")
    by_id = {r.series_id: r for r in summary.itertuples()}

    assert str(by_id["CPIAUCSL"].latest_date)[:10] == "2024-02-01"
    assert by_id["CPIAUCSL"].latest_value == pytest.approx(301.5)
    assert str(by_id["GDPC1"].latest_date)[:10] == "2024-01-01"
    assert by_id["GDPC1"].latest_value == pytest.approx(22100.0)


# ==========================================================
# Date typing (the SQLite CAST trap regression guard)
# ==========================================================

def test_observation_date_preserved_not_corrupted(raw_loaded_engine):
    """business-correctness: observation_date must stay the full ISO date.

    Guards the SQLite trap where CAST('2024-01-01' AS DATE) collapses to the
    integer 2024. A mart observation_date reading 2024 instead of 2024-01-01
    would pass a careless row-count check while serving corrupt dates.
    """
    marts.build_marts(raw_loaded_engine)
    rows = _rows(raw_loaded_engine, "public_analytics.mart_inflation")
    dates = {str(d)[:10] for d in rows["observation_date"]}
    assert dates == {"2024-01-01", "2024-02-01"}


# ==========================================================
# Idempotency
# ==========================================================

def test_build_marts_is_idempotent(raw_loaded_engine):
    """business-correctness: a second build produces identical marts — no
    duplicate-key error and no row duplication (full-refresh semantics)."""
    first = marts.build_marts(raw_loaded_engine)
    second = marts.build_marts(raw_loaded_engine)
    assert first == second
    # And the physical tables hold the same counts the stats report.
    for table, count in second.items():
        physical = "staging.stg_economic_observations" if table == "staging" \
            else f"public_analytics.{table}"
        assert len(_rows(raw_loaded_engine, physical)) == count


def test_build_marts_reflects_revised_raw_value(raw_loaded_engine):
    """business-correctness: rebuilding after a raw value changes refreshes
    the mart — the marts are derived state, not a one-time copy."""
    marts.build_marts(raw_loaded_engine)
    with raw_loaded_engine.connect() as conn:
        conn.execute(text(
            "UPDATE raw.fact_economic_observations SET value = 999.0 "
            "WHERE series_id = 'CPIAUCSL' AND date = '2024-02-01'"
        ))
        conn.commit()
    marts.build_marts(raw_loaded_engine)

    summary = _rows(raw_loaded_engine, "public_analytics.mart_economic_summary")
    cpi = {r.series_id: r for r in summary.itertuples()}["CPIAUCSL"]
    assert cpi.latest_value == pytest.approx(999.0)


# ==========================================================
# API-contract alignment
# ==========================================================

def test_domain_mart_columns_and_pk_match_api_contract(raw_loaded_engine):
    """structural: the domain marts expose exactly the columns and the
    (series_id, observation_date) PK the API's SQLAlchemy models declare, so
    the API can read them without a schema mismatch."""
    marts.build_marts(raw_loaded_engine)
    insp = inspect(raw_loaded_engine)
    expected_cols = {"series_id", "observation_date", "series_name", "value", "source"}
    for table in ("mart_inflation", "mart_labor_market", "mart_gdp"):
        cols = {c["name"] for c in insp.get_columns(table, schema="public_analytics")}
        assert cols == expected_cols, table
        pk = set(insp.get_pk_constraint(table, schema="public_analytics")
                 ["constrained_columns"])
        assert pk == {"series_id", "observation_date"}, table


def test_summary_columns_and_pk_match_api_contract(raw_loaded_engine):
    """structural: mart_economic_summary exposes the latest-value rollup
    columns and the single series_id PK the API's MartEconomicSummary model
    declares — a different shape from the domain marts, matched exactly."""
    marts.build_marts(raw_loaded_engine)
    insp = inspect(raw_loaded_engine)
    cols = {c["name"] for c in
            insp.get_columns("mart_economic_summary", schema="public_analytics")}
    assert cols == {"series_id", "series_name", "source", "latest_date", "latest_value"}
    pk = set(insp.get_pk_constraint("mart_economic_summary", schema="public_analytics")
             ["constrained_columns"])
    assert pk == {"series_id"}


def test_api_inflation_query_shape_succeeds(raw_loaded_engine):
    """structural: the exact SELECT the API issues against mart_inflation
    (ordered by series_id, observation_date) runs and returns the domain rows,
    standing in for the API reading the mart end-to-end."""
    marts.build_marts(raw_loaded_engine)
    with raw_loaded_engine.connect() as conn:
        result = conn.execute(text(
            "SELECT series_id, observation_date, series_name, value, source "
            "FROM public_analytics.mart_inflation "
            "ORDER BY series_id, observation_date"
        )).fetchall()
    assert {r[0] for r in result} == _DOMAIN_SERIES["mart_inflation"]


# ==========================================================
# Mapping integrity (config grounded in real ingested series)
# ==========================================================

def test_mart_domains_reference_only_ingested_series():
    """business-correctness: every series_id in MART_DOMAINS is an actually
    ingested FRED or BLS series, so the routing config cannot drift to name a
    series the pipeline never lands."""
    ingested = set(FRED_SERIES.values()) | set(BLS_SERIES.values())
    mapped = {sid for ids in MART_DOMAINS.values() for sid in ids}
    assert mapped <= ingested, mapped - ingested


def test_mart_domains_have_no_duplicate_series():
    """business-correctness: no series_id is mapped to two domains, so the
    domain marts cannot double-count a series."""
    all_ids = [sid for ids in MART_DOMAINS.values() for sid in ids]
    assert len(all_ids) == len(set(all_ids))
