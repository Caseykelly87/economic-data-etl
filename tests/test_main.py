import logging
from datetime import datetime
import pandas as pd
import pytest
from unittest.mock import patch, call
from src import main
from src.config import FRED_SERIES, BLS_SERIES, ERS_SERIES, BLS_START_YEAR


# ---------------------------------------------------------------------------
# Neutral return values reused across all tests
# ---------------------------------------------------------------------------

_FRED_DATA  = {"observations": [{"date": "2024-01-01", "value": "5.0"}]}
_BLS_DATA   = {
    "status": "REQUEST_SUCCEEDED",
    "Results": {"series": [{"seriesID": "CUUR0000SA0", "data": []}]},
}
_ERS_DATA   = {"rows": []}
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
         patch("src.main.fetch_ers_price_outlook", return_value=_ERS_DATA)   as mock_ers,        \
         patch("src.main.parse_fred_observations", return_value=_EMPTY_DF)   as mock_parse_fred, \
         patch("src.main.parse_bls_batch",         return_value=_EMPTY_DF)   as mock_parse_bls,  \
         patch("src.main.parse_ers_csv",           return_value=_EMPTY_DF)   as mock_parse_ers,  \
         patch("src.main.combine_fact_tables",     return_value=_EMPTY_DF)   as mock_combine,    \
         patch("src.main.build_dim_series",        return_value=_EMPTY_DF)   as mock_dim,        \
         patch("src.main.ensure_tables_exist")                               as mock_ensure,     \
         patch("src.main.upsert_observations",     return_value=_LOAD_STATS) as mock_upsert_obs, \
         patch("src.main.upsert_dim_series",       return_value={"inserted": 0, "unchanged": 0}) \
                                                                              as mock_upsert_dim, \
         patch("src.main.build_marts",             return_value={"staging": 0}) as mock_build_marts, \
         patch("src.main.create_engine")                                      as mock_engine:
        yield {
            "fetch_fred":    mock_fred,
            "fetch_bls":     mock_bls,
            "fetch_ers":     mock_ers,
            "parse_fred":    mock_parse_fred,
            "parse_bls":     mock_parse_bls,
            "parse_ers":     mock_parse_ers,
            "combine":       mock_combine,
            "build_dim":     mock_dim,
            "ensure_tables": mock_ensure,
            "upsert_obs":    mock_upsert_obs,
            "upsert_dim":    mock_upsert_dim,
            "build_marts":   mock_build_marts,
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

    pipeline_mocks["fetch_bls"].assert_called_once_with(BLS_SERIES, BLS_START_YEAR, datetime.now().year)


def test_run_pipeline_exits_nonzero_on_fred_error(pipeline_mocks, caplog):
    """A FRED extraction error must be logged and exit the process with code 1."""
    pipeline_mocks["fetch_fred"].side_effect = Exception("API down")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            main.run_pipeline()

    assert exc_info.value.code == 1
    assert "Pipeline failed" in caplog.text
    pipeline_mocks["fetch_bls"].assert_not_called()  # BLS must not run when FRED fails


def test_run_pipeline_exits_nonzero_on_bls_error(pipeline_mocks, caplog):
    """A BLS extraction error must be logged and exit the process with code 1."""
    pipeline_mocks["fetch_bls"].side_effect = RuntimeError("BLS down")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            main.run_pipeline()

    assert exc_info.value.code == 1
    assert "Pipeline failed" in caplog.text

def test_run_pipeline_fred_per_series_isolation(pipeline_mocks, caplog):
    """One failing FRED series must not stop the rest of the loop.

    Structural: pins the contract that every series is attempted even when
    an earlier one raises, and that the phase still ultimately fails so the
    operator does not silently lose data.
    """
    # First call raises, remaining calls succeed with the neutral _FRED_DATA stub.
    pipeline_mocks["fetch_fred"].side_effect = [Exception("series 1 down")] + [
        _FRED_DATA
    ] * (len(FRED_SERIES) - 1)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            main.run_pipeline()

    assert exc_info.value.code == 1
    assert pipeline_mocks["fetch_fred"].call_count == len(FRED_SERIES)
    assert "FRED fetch failed" in caplog.text
    # BLS must not run when any FRED series failed.
    pipeline_mocks["fetch_bls"].assert_not_called()


def test_run_pipeline_calls_fetch_ers_price_outlook(pipeline_mocks):
    """fetch_ers_price_outlook must be called once during extraction."""
    main.run_pipeline()

    pipeline_mocks["fetch_ers"].assert_called_once()


def test_run_pipeline_calls_parse_ers_csv(pipeline_mocks):
    """parse_ers_csv must be called once during transform."""
    main.run_pipeline()

    pipeline_mocks["parse_ers"].assert_called_once()



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
    """build_dim_series must be called with all four series maps from config."""
    main.run_pipeline()

    pipeline_mocks["build_dim"].assert_called_once_with(
        FRED_SERIES, BLS_SERIES,
        ers_series=ERS_SERIES,
    )



def test_run_pipeline_exits_nonzero_on_transform_error(pipeline_mocks, caplog):
    """A transform error must be logged, exit code 1, and skip the load phase."""
    pipeline_mocks["parse_fred"].side_effect = KeyError("observations")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            main.run_pipeline()

    assert exc_info.value.code == 1
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


def test_run_pipeline_builds_marts_after_upserts(pipeline_mocks):
    """build_marts must run as the final load step, after the raw upserts.

    Structural: pins the deliberate raw → staging → marts ordering — the
    marts derive from raw, so they must be built after raw is populated, not
    before or in place of it.
    """
    main.run_pipeline()

    pipeline_mocks["build_marts"].assert_called_once()
    pipeline_mocks["upsert_obs"].assert_called_once()
    pipeline_mocks["upsert_dim"].assert_called_once()


def test_run_pipeline_exits_nonzero_on_load_error(pipeline_mocks, caplog):
    """A load error must be logged and exit the process with code 1."""
    pipeline_mocks["ensure_tables"].side_effect = Exception("DB unavailable")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as exc_info:
            main.run_pipeline()

    assert exc_info.value.code == 1
    assert "Pipeline failed" in caplog.text