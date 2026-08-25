from pathlib import Path

import pytest

from src import extract


@pytest.fixture
def temp_dirs(tmp_path, monkeypatch):
    """
    Redirect raw and metadata directories to pytest temp dir so tests
    never touch real project data folders.
    """
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"

    raw_dir.mkdir()
    metadata_dir.mkdir()

    monkeypatch.setattr(extract, "DATA_RAW_DIR", raw_dir)
    monkeypatch.setattr(extract, "DATA_METADATA_DIR", metadata_dir)

    return raw_dir, metadata_dir


@pytest.fixture
def mock_fred_response():
    """Minimal valid FRED API response."""
    return {
        "observations": [
            {"date": "2024-01-01", "value": "5.0"},
            {"date": "2024-02-01", "value": "5.1"},
        ]
    }


@pytest.fixture
def mock_bls_response():
    """Minimal valid BLS API response."""
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": "TEST123",
                    "data": [{"year": "2024", "period": "M01", "value": "100"}],
                }
            ]
        },
    }

# ==========================================================
# Transform Layer Fixtures
# These represent realistic raw JSON as saved to data/raw/,
# including edge cases the transform layer must handle.
# ==========================================================

@pytest.fixture
def raw_fred_json():
    """
    Realistic FRED API response including extra metadata fields and one
    missing value represented as "." — a FRED-specific quirk that must
    become NaN, not cause a parse error.
    """
    return {
        "realtime_start": "2024-01-01",
        "realtime_end": "9999-12-31",
        "observation_start": "2024-01-01",
        "observation_end": "9999-12-31",
        "units": "Percent",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 3,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {"date": "2024-01-01", "value": "5.0",  "realtime_start": "2024-01-01", "realtime_end": "9999-12-31"},  # noqa: E501
            {"date": "2024-02-01", "value": ".",     "realtime_start": "2024-02-01", "realtime_end": "9999-12-31"},  # noqa: E501
            {"date": "2024-03-01", "value": "5.2",  "realtime_start": "2024-03-01", "realtime_end": "9999-12-31"},  # noqa: E501
        ],
    }


@pytest.fixture
def raw_bls_json():
    """
    Realistic BLS API batch response with two series.
    Note: BLS returns data most-recent-first within each series.
    The transform layer must normalise to oldest-first.
    """
    return {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 150,
        "message": [],
        "Results": {
            "series": [
                {
                    "seriesID": "CUUR0000SA0",
                    "data": [
                        {"year": "2024", "period": "M03", "periodName": "March",    "value": "314.2", "footnotes": [{}]},  # noqa: E501
                        {"year": "2024", "period": "M02", "periodName": "February", "value": "313.5", "footnotes": [{}]},  # noqa: E501
                        {"year": "2024", "period": "M01", "periodName": "January",  "value": "312.0", "footnotes": [{}]},  # noqa: E501
                    ],
                },
                {
                    "seriesID": "CES0500000003",
                    "data": [
                        {"year": "2024", "period": "M03", "periodName": "March",    "value": "34.85", "footnotes": [{}]},  # noqa: E501
                        {"year": "2024", "period": "M02", "periodName": "February", "value": "34.75", "footnotes": [{}]},  # noqa: E501
                        {"year": "2024", "period": "M01", "periodName": "January",  "value": "34.55", "footnotes": [{}]},  # noqa: E501
                    ],
                },
            ]
        },
    }


# ==========================================================
# Load Layer Fixtures
# ==========================================================

@pytest.fixture
def db_engine():
    """
    Isolated SQLite in-memory engine, fresh for every test.
    StaticPool ensures all connections within one engine see the same
    in-memory database — required for SQLite :memory: to work correctly
    with SQLAlchemy's connection pool.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    yield engine
    engine.dispose()


@pytest.fixture
def sample_observations_df():
    """
    Small fact DataFrame matching the schema produced by transform.
    Includes one NaN value to exercise NULL handling.
    """
    import pandas as pd

    return pd.DataFrame({
        "series_id":   ["UNRATE",  "UNRATE",  "FEDFUNDS"],
        "series_name": ["UNRATE",  "UNRATE",  "MONEY_COST"],
        "date":        pd.to_datetime(["2024-01-01", "2024-02-01", "2024-01-01"]),
        "value":       [4.0,       float("nan"), 5.33],
        "source":      ["FRED",    "FRED",    "FRED"],
    })


@pytest.fixture
def sample_dim_df():
    """
    Small dimension DataFrame matching the schema produced by
    transform.build_dim_series.
    """
    import pandas as pd

    return pd.DataFrame({
        "series_id":   ["UNRATE",  "FEDFUNDS",   "CUUR0000SA0"],
        "series_name": ["UNRATE",  "MONEY_COST", "CPI_URBAN"],
        "source":      ["FRED",    "FRED",        "BLS"],
    })


@pytest.fixture
def mock_ers_response():
    """Minimal valid ERS CPI Forecasts payload (dict with 'rows' list of dicts)."""
    return {
        "rows": [
            {"Category": "All food",          "Year": "2024", "Annual": "2.1"},
            {"Category": "Food at home",       "Year": "2024", "Annual": "1.8"},
            {"Category": "Food away from home","Year": "2024", "Annual": "4.2"},
        ]
    }


# ==========================================================
# Sim Engine Ingestion Fixtures
# On-disk CSV trees under tests/fixtures/sim_engine/ that mirror the
# sim engine's output/ layout. See the fixture directories themselves
# for the actual row contents.
# ==========================================================

def _sim_fixture_root(request, variant: str) -> Path:
    return (
        Path(request.config.rootdir)
        / "tests"
        / "fixtures"
        / "sim_engine"
        / variant
        / "output"
    )


@pytest.fixture
def sim_happy_root(request):
    """Path to the happy-path sim engine output tree (3 dates, 8 stores each)."""
    return _sim_fixture_root(request, "happy")


@pytest.fixture
def sim_corrupt_root(request):
    """Path to the corrupt sim engine tree: one store_summary.csv missing net_sales_total."""
    return _sim_fixture_root(request, "corrupt_missing_column")


@pytest.fixture
def sim_partial_root(request):
    """Path to the partial sim engine tree: one date directory has no store_summary.csv."""
    return _sim_fixture_root(request, "partial_missing_date")


@pytest.fixture
def sim_anomalous_root(request):
    """Path to the anomalous sim engine tree: deliberate injections on 2024-06-16.

    See ``tests/fixtures/sim_engine/anomalous/README.md`` for the exact
    rows and which rules each is expected to fire.
    """
    return _sim_fixture_root(request, "anomalous")


# ==========================================================
# Exception Detection Fixtures
# In-memory and on-disk inputs for the phase 2 rules engine.
# ==========================================================

@pytest.fixture
def detection_rules_config(request):
    """Loaded dict from config/detection_rules.yaml — the full rules tree."""
    import yaml

    path = Path(request.config.rootdir) / "config" / "detection_rules.yaml"
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def sample_dim_stores_df():
    """Eight-store dim_stores DataFrame matching the canonical seed config.

    Carries store_id, base_daily_revenue, and trade_area_profile —
    every field the detection rules dereference. Other columns from
    the on-disk dim_stores.csv are omitted as irrelevant to the rules.
    """
    import pandas as pd

    return pd.DataFrame({
        "store_id":           [1, 2, 3, 4, 5, 6, 7, 8],
        "base_daily_revenue": [95000.0, 110000.0, 85000.0,
                               68000.0, 58000.0, 62000.0,
                               55000.0, 52000.0],
        "trade_area_profile": ["suburban-family"] * 3
                              + ["urban-dense"] * 3
                              + ["value-market"] * 2,
    })


@pytest.fixture
def sample_metrics_df():
    """In-memory store_daily_metrics DataFrame matching the six-column schema.

    One row per (date, store_id) for two consecutive days so YoY logic
    can be exercised independently by tests that supply a T-365 row.
    All values are profile-typical so no rule fires by default; tests
    mutate specific cells before invoking the rules engine.
    """
    from datetime import date as _date

    import numpy as np
    import pandas as pd

    rows = []
    profiles_avg = {1: 38.0, 2: 38.0, 3: 38.0,
                    4: 28.0, 5: 28.0, 6: 28.0,
                    7: 32.0, 8: 32.0}
    profile_pct = {1: 0.105, 2: 0.105, 3: 0.105,
                   4: 0.115, 5: 0.115, 6: 0.115,
                   7: 0.120, 8: 0.120}
    base = {1: 95000.0, 2: 110000.0, 3: 85000.0,
            4: 68000.0, 5: 58000.0, 6: 62000.0,
            7: 55000.0, 8: 52000.0}
    for d in (_date(2024, 6, 15), _date(2024, 6, 16)):
        for sid in range(1, 9):
            sales = base[sid] * 0.92
            txns = int(round(sales / profiles_avg[sid]))
            rows.append({
                "date": d,
                "store_id": sid,
                "total_sales": sales,
                "transaction_count": txns,
                "avg_basket_size": sales / txns,
                "labor_cost_pct": profile_pct[sid],
            })
    df = pd.DataFrame(rows)
    df["store_id"] = df["store_id"].astype(np.int64)
    df["transaction_count"] = df["transaction_count"].astype(np.int64)
    return df
