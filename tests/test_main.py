import logging
import pandas as pd
import pytest
from unittest.mock import patch, call
from src import main
from src.config import FRED_SERIES, BLS_SERIES


# ---------------------------------------------------------------------------
# Neutral return values reused across all tests
# ---------------------------------------------------------------------------

_FRED_DATA  = {"observations": [{"date": "2024-01-01", "value": "5.0"}]}
_BLS_DATA   = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {"series": [{"seriesID": "CUUR0000SA0", "data": []}]},
}
_EMPTY_DF   = pd.DataFrame(columns=["series_id", "series_name", "date", "value", "source"])
_LOAD_STATS = {"inserted": 0, "updated": 0, "unchanged": 0}


@pytest.fixture
def pipeline_mocks():
    """
    Neutral stubs for every I/O-touching function in run_pipeline.

    Prevents real API calls, file writes, and database access during unit
    tests. Each test receives the mock dict and may set side_effect or
    assert_called on individual mocks.
    """
    with patch("src.main.fetch_fred_data",        return_value=_FRED_DATA)  as mock_fred,       \
         patch("src.main.fetch_bls_data",          return_value=_BLS_DATA)   as mock_bls,        \
         patch("src.main.parse_fred_observations", return_value=_EMPTY_DF)   as mock_parse_fred, \
         patch("src.main.parse_bls_batch",         return_value=_EMPTY_DF)   as mock_parse_bls,  \
         patch("src.main.combine_fact_tables",     return_value=_EMPTY_DF)   as mock_combine,    \
         patch("src.main.build_dim_series",        return_value=_EMPTY_DF)   as mock_dim,        \
         patch("src.main.ensure_tables_exist")                               as mock_ensure,     \
         patch("src.main.upsert_observations",     return_value=_LOAD_STATS) as mock_upsert_obs, \
         patch("src.main.upsert_dim_series",       return_value={"inserted": 0, "unchanged": 0}) \
                                                                              as mock_upsert_dim, \
         patch("src.main.create_engine")                                      as mock_engine:
        yield {
            "fetch_fred":    mock_fred,
            "fetch_bls":     mock_bls,
            "parse_fred":    mock_parse_fred,
            "parse_bls":     mock_parse_bls,
            "combine":       mock_combine,
            "build_dim":     mock_dim,
            "ensure_tables": mock_ensure,
            "upsert_obs":    mock_upsert_obs,
            "upsert_dim":    mock_upsert_dim,
            "create_engine": mock_engine,
        }


# ==========================================================
# Extract Phase Tests
# ==========================================================

def test_run_pipeline_calls_fred_for_every_series(pipeline_mocks):
    """run_pipeline must call fetch_fred_data once per configured FRED series."""
    main.run_pipeline()

    assert pipeline_mocks["fetch_fred"].call_count == len(FRED_SERIES)
    expected_calls = [call(sid) for sid in FRED_SERIES.values()]
    pipeline_mocks["fetch_fred"].assert_has_calls(expected_calls, any_order=False)


def test_run_pipeline_calls_bls_with_correct_args(pipeline_mocks):
    """run_pipeline must call fetch_bls_data with BLS_SERIES and the configured year range."""
    main.run_pipeline()

    pipeline_mocks["fetch_bls"].assert_called_once_with(BLS_SERIES, 2021, 2026)


def test_run_pipeline_handles_fred_error_gracefully(pipeline_mocks, caplog):
    """An extraction error must be logged and must not raise out of run_pipeline."""
    pipeline_mocks["fetch_fred"].side_effect = Exception("API down")

    with caplog.at_level(logging.ERROR):
        main.run_pipeline()  # Must not raise

    assert "Pipeline failed" in caplog.text
    pipeline_mocks["fetch_bls"].assert_not_called()  # BLS must not run when FRED fails


def test_run_pipeline_handles_bls_error_gracefully(pipeline_mocks, caplog):
    """A BLS extraction error must be logged and must not raise out of run_pipeline."""
    pipeline_mocks["fetch_bls"].side_effect = RuntimeError("BLS down")

    with caplog.at_level(logging.ERROR):
        main.run_pipeline()  # Must not raise

    assert "Pipeline failed" in caplog.text


# ==========================================================
# Transform Phase Tests
# ==========================================================

def test_run_pipeline_calls_parse_fred_for_each_series(pipeline_mocks):
    """parse_fred_observations must be called once per FRED series with its data and IDs."""
    main.run_pipeline()

    assert pipeline_mocks["parse_fred"].call_count == len(FRED_SERIES)
    for name, series_id in FRED_SERIES.items():
        pipeline_mocks["parse_fred"].assert_any_call(_FRED_DATA, series_id, name)


def test_run_pipeline_calls_parse_bls_once(pipeline_mocks):
    """parse_bls_batch must be called once with the full BLS response and series map."""
    main.run_pipeline()

    pipeline_mocks["parse_bls"].assert_called_once_with(_BLS_DATA, BLS_SERIES)


def test_run_pipeline_calls_combine_fact_tables(pipeline_mocks):
    """combine_fact_tables must be called to merge FRED and BLS DataFrames."""
    main.run_pipeline()

    pipeline_mocks["combine"].assert_called_once()


def test_run_pipeline_calls_build_dim_series(pipeline_mocks):
    """build_dim_series must be called with the FRED and BLS series maps from config."""
    main.run_pipeline()

    pipeline_mocks["build_dim"].assert_called_once_with(FRED_SERIES, BLS_SERIES)


def test_run_pipeline_handles_transform_error_gracefully(pipeline_mocks, caplog):
    """A transform error must be logged and load must not run."""
    pipeline_mocks["parse_fred"].side_effect = KeyError("observations")

    with caplog.at_level(logging.ERROR):
        main.run_pipeline()  # Must not raise

    assert "Pipeline failed" in caplog.text
    pipeline_mocks["ensure_tables"].assert_not_called()


# ==========================================================
# Load Phase Tests
# ==========================================================

def test_run_pipeline_calls_ensure_tables_exist(pipeline_mocks):
    """ensure_tables_exist must be called before any upsert operations."""
    main.run_pipeline()

    pipeline_mocks["ensure_tables"].assert_called_once()


def test_run_pipeline_calls_upsert_observations(pipeline_mocks):
    """upsert_observations must be called with the combined fact DataFrame."""
    main.run_pipeline()

    pipeline_mocks["upsert_obs"].assert_called_once()


def test_run_pipeline_calls_upsert_dim_series(pipeline_mocks):
    """upsert_dim_series must be called with the dimension DataFrame."""
    main.run_pipeline()

    pipeline_mocks["upsert_dim"].assert_called_once()


def test_run_pipeline_handles_load_error_gracefully(pipeline_mocks, caplog):
    """A load error must be logged and must not raise out of run_pipeline."""
    pipeline_mocks["ensure_tables"].side_effect = Exception("DB unavailable")

    with caplog.at_level(logging.ERROR):
        main.run_pipeline()  # Must not raise

    assert "Pipeline failed" in caplog.text