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
            {"date": "2024-01-01", "value": "5.0",  "realtime_start": "2024-01-01", "realtime_end": "9999-12-31"},
            {"date": "2024-02-01", "value": ".",     "realtime_start": "2024-02-01", "realtime_end": "9999-12-31"},
            {"date": "2024-03-01", "value": "5.2",  "realtime_start": "2024-03-01", "realtime_end": "9999-12-31"},
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
                        {"year": "2024", "period": "M03", "periodName": "March",    "value": "314.2", "footnotes": [{}]},
                        {"year": "2024", "period": "M02", "periodName": "February", "value": "313.5", "footnotes": [{}]},
                        {"year": "2024", "period": "M01", "periodName": "January",  "value": "312.0", "footnotes": [{}]},
                    ],
                },
                {
                    "seriesID": "CES0500000003",
                    "data": [
                        {"year": "2024", "period": "M03", "periodName": "March",    "value": "34.85", "footnotes": [{}]},
                        {"year": "2024", "period": "M02", "periodName": "February", "value": "34.75", "footnotes": [{}]},
                        {"year": "2024", "period": "M01", "periodName": "January",  "value": "34.55", "footnotes": [{}]},
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